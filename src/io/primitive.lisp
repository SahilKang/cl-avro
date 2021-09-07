;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2021 Google LLC
;;;
;;; This file is part of cl-avro.
;;;
;;; cl-avro is free software: you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation, either version 3 of the License, or
;;; (at your option) any later version.
;;;
;;; cl-avro is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with cl-avro.  If not, see <http://www.gnu.org/licenses/>.

(in-package #:cl-user)
(defpackage #:cl-avro.io.primitive
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:serialized-size
                #:serialize-into-vector
                #:serialize-into-stream
                #:define-serialize-into
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from)
  (:export #:serialize-into-vector
           #:serialize-into-stream
           #:serialized-size
           #:deserialize-from-vector
           #:deserialize-from-stream
           #:little-endian->uint32
           #:little-endian->uint64
           #:uint32->little-endian
           #:uint64->little-endian))
(in-package #:cl-avro.io.primitive)

;;; null schema

(defmethod serialized-size ((object null))
  0)

(define-serialize-into null
  "Write zero bytes."
  `(declare (ignore object ,@(if vectorp '(vector start) '(stream))))
  0)

(define-deserialize-from (eql 'schema:null)
  "Read zero bytes and return nil."
  `(declare (ignore schema ,@(if vectorp '(vector start) '(stream))))
  `(values nil 0))

;;; boolean schema

(defmethod serialized-size ((object (eql 'schema:true)))
  1)

(defmethod serialized-size ((object (eql 'schema:false)))
  1)

(define-serialize-into (eql 'schema:true)
  "Write #o1."
  (declare (ignore object))
  `(prog1 1
     ,(if vectorp
          '(setf (elt vector start) 1)
          '(write-byte 1 stream))))

(define-serialize-into (eql 'schema:false)
  "Write #o0."
  (declare (ignore object))
  `(prog1 1
     ,(if vectorp
          '(setf (elt vector start) 0)
          '(write-byte 0 stream))))

(define-deserialize-from (eql 'schema:boolean)
  "Read a byte and return TRUE if it's 1, or FALSE if it's 0."
  (declare (ignore schema))
  `(values
    (ecase ,(if vectorp
                `(elt vector start)
                `(read-byte stream))
      (0 'schema:false)
      (1 'schema:true))
    1))

;;; int and long schemas

;; from zig-zag

(declaim
 (ftype (function ((unsigned-byte 32)) (values schema:int &optional))
        zig-zag->int)
 (inline zig-zag->int))
(defun zig-zag->int (zig-zag)
  (logxor (ash zig-zag -1) (- (logand zig-zag 1))))
(declaim (notinline zig-zag->int))

(declaim
 (ftype (function ((unsigned-byte 64)) (values schema:long &optional))
        zig-zag->long)
 (inline zig-zag->long))
(defun zig-zag->long (zig-zag)
  (logxor (ash zig-zag -1) (- (logand zig-zag 1))))
(declaim (notinline zig-zag->long))

;; to zig-zag

(declaim
 (ftype (function (schema:int) (values (unsigned-byte 32) &optional))
        int->zig-zag)
 (inline int->zig-zag))
(defun int->zig-zag (int)
  (logxor (ash int 1) (ash int -31)))
(declaim (notinline int->zig-zag))

(declaim
 (ftype (function (schema:long) (values (unsigned-byte 64) &optional))
        long->zig-zag)
 (inline long->zig-zag))
(defun long->zig-zag (long)
  (logxor (ash long 1) (ash long -63)))
(declaim (notinline long->zig-zag))

;; serialized-size

(defmethod serialized-size ((object integer))
  (loop
    with zig-zag = (etypecase object
                     (schema:int (int->zig-zag object))
                     (schema:long (long->zig-zag object)))

    for integer = zig-zag then (ash integer -7)
    until (zerop (logand integer (lognot #x7f)))
    count integer into length

    finally
       (return (1+ length))))

;; variable length integers

(defmacro read-variable-length-integer (input bits &optional (start 0))
  (declare ((member stream vector) input)
           ((member 32 64) bits))
  (let* ((max-bytes (ceiling bits 7))
         (type (ecase bits (32 "int") (64 "long")))
         (error-message
           (format nil "Too many bytes for ~A, expected ~S bytes max"
                   type max-bytes)))
    `(loop
       with value of-type (unsigned-byte ,bits) = 0

       for offset of-type fixnum below ,max-bytes
       ,@(when (eq input 'vector)
           `(for index of-type fixnum = ,start then (1+ index)))
       for byte of-type (unsigned-byte 8) = ,(if (eq input 'vector)
                                                 `(elt ,input index)
                                                 `(read-byte ,input))

       do (setf value (logior value (ash (logand byte #x7f)
                                         (* 7 offset))))

       when (zerop (logand byte #x80))
         return (values value (1+ offset))

       finally
          (error ,error-message))))

(defmacro write-variable-length-integer
    (bits integer into &optional (start nil startp))
  (declare (symbol into start)
           ((member 32 64) bits))
  `(loop
     for integer of-type (unsigned-byte ,bits) = ,integer then (ash integer -7)
     ,@(when startp
         `(for index of-type fixnum = ,start then (1+ index)))
     until (zerop (logand integer (lognot #x7f)))
     for byte = (logior (logand integer #x7f) #x80)
     ,@(unless startp
         `(count 1 into count))

     do ,(if startp
             `(setf (elt ,into index) byte)
             `(write-byte byte ,into))

     finally
     ,@(if startp
           `((setf (elt ,into index) (logand integer #xff))
             (return (the fixnum (1+ (- index start)))))
           `((write-byte (logand integer #xff) ,into)
             (return (the fixnum (1+ count)))))))

;; serialize

(define-serialize-into integer
  "Write OBJECT as a variable-length zig-zag integer."
  (declare (inline int->zig-zag long->zig-zag))
  (let ((args (if vectorp '(vector start) '(stream))))
    `(etypecase object
       (schema:int
        (write-variable-length-integer 32 (int->zig-zag object) ,@args))
       (schema:long
        (write-variable-length-integer 64 (long->zig-zag object) ,@args)))))

;; deserialize int

(define-deserialize-from (eql 'schema:int)
  "Read a 32-bit variable-length zig-zag integer."
  (declare (ignore schema)
           (inline zig-zag->int))
  `(multiple-value-bind (zig-zag bytes-read)
       (read-variable-length-integer
           ,(if vectorp 'vector 'stream) 32 ,@(when vectorp '(start)))
     (values (zig-zag->int zig-zag) bytes-read)))

;; deserialize long

(define-deserialize-from (eql 'schema:long)
  "Read a 64-bit variable-length zig-zag integer."
  (declare (ignore schema)
           (inline zig-zag->long))
  `(multiple-value-bind (zig-zag bytes-read)
       (read-variable-length-integer
           ,(if vectorp 'vector 'stream) 64 ,@(when vectorp '(start)))
     (values (zig-zag->long zig-zag) bytes-read)))

;;; float and double schemas

;; from little-endian

(defmacro from-little-endian (bits bytes start)
  (declare ((member 32 64) bits)
           (symbol bytes start))
  (let ((value (gensym))
        (offset (gensym))
        (index (gensym))
        (byte (gensym)))
    `(loop
       with ,value of-type (unsigned-byte ,bits) = 0

       for ,offset of-type fixnum below ,(truncate bits 8)
       for ,index of-type fixnum = ,start then (1+ ,index)
       for ,byte of-type (unsigned-byte 8) = (elt ,bytes ,index)

       do (setf ,value (logior ,value (ash ,byte (* ,offset 8))))

       finally
          (return ,value))))

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)) &optional fixnum)
                  (values (unsigned-byte 32) &optional))
        little-endian->uint32)
 (inline little-endian->uint32))
(defun little-endian->uint32 (bytes &optional (start 0))
  (from-little-endian 32 bytes start))
(declaim (notinline little-endian->uint32))

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)) &optional fixnum)
                  (values (unsigned-byte 64) &optional))
        little-endian->uint64)
 (inline little-endian->uint64))
(defun little-endian->uint64 (bytes &optional (start 0))
  (from-little-endian 64 bytes start))
(declaim (notinline little-endian->uint64))

;; to little-endian

(defmacro to-little-endian (bits integer into &optional (start nil startp))
  (declare ((member 32 64) bits)
           (symbol integer into start))
  (let* ((offset (gensym))
         (byte (gensym))
         (index (gensym)))
    `(loop
       for ,offset of-type fixnum below ,(truncate bits 8)
       for ,byte of-type (unsigned-byte 8) = (logand #xff (ash ,integer
                                                               (* ,offset -8)))
       ,@(when startp
           `(for ,index of-type fixnum = ,start then (1+ ,index)))

       do ,(if startp
               `(setf (elt ,into ,index) ,byte)
               `(write-byte ,byte ,into))

       finally
          (return (values)))))

(declaim
 (ftype (function ((unsigned-byte 32)
                   (or stream (simple-array (unsigned-byte 8) (*)))
                   &optional fixnum)
                  (values &optional))
        uint32->little-endian)
 (inline uint32->little-endian))
(defun uint32->little-endian (integer into &optional (start 0))
  (etypecase into
    (stream
     (to-little-endian 32 integer into))
    ((simple-array (unsigned-byte 8) (*))
     (to-little-endian 32 integer into start))))
(declaim (notinline uint32->little-endian))

(declaim
 (ftype (function ((unsigned-byte 64)
                   (or stream (simple-array (unsigned-byte 8) (*)))
                   &optional fixnum)
                  (values &optional))
        uint64->little-endian)
 (inline uint64->little-endian))
(defun uint64->little-endian (integer into &optional (start 0))
  (etypecase into
    (stream
     (to-little-endian 64 integer into))
    ((simple-array (unsigned-byte 8) (*))
     (to-little-endian 64 integer into start))))
(declaim (notinline uint64->little-endian))

;; float schema

(defmethod serialized-size ((object single-float))
  4)

(define-serialize-into single-float
  "Write single-precision float."
  (declare (inline uint32->little-endian ieee-floats:encode-float32))
  (let ((args (if vectorp '(vector start) '(stream))))
    `(prog1 4
       (let ((integer (ieee-floats:encode-float32 object)))
         (uint32->little-endian integer ,@args)))))

(define-deserialize-from (eql 'schema:float)
  "Read a single-precision float."
  (declare (ignore schema)
           (inline little-endian->uint32 ieee-floats:decode-float32))
  `(values
    (ieee-floats:decode-float32
     (little-endian->uint32
      ,(if vectorp
           'vector
           `(let ((bytes (make-array 4 :element-type '(unsigned-byte 8))))
              (unless (= (read-sequence bytes stream) 4)
                (error 'end-of-file :stream *error-output*))
              bytes))
      ,@(when vectorp '(start))))
    4))

;; double schema

(defmethod serialized-size ((object double-float))
  8)

(define-serialize-into double-float
  "Write double-precision float."
  (declare (inline uint64->little-endian ieee-floats:encode-float64))
  (let ((args (if vectorp '(vector start) '(stream))))
    `(prog1 8
       (let ((integer (ieee-floats:encode-float64 object)))
         (uint64->little-endian integer ,@args)))))

(define-deserialize-from (eql 'schema:double)
  "Read a double-precision float."
  (declare (ignore schema)
           (inline little-endian->uint64 ieee-floats:decode-float64))
  `(values
    (ieee-floats:decode-float64
     (little-endian->uint64
      ,(if vectorp
           'vector
           `(let ((bytes (make-array 8 :element-type '(unsigned-byte 8))))
              (unless (= (read-sequence bytes stream) 8)
                (error 'end-of-file :stream *error-output*))
              bytes))
      ,@(when vectorp '(start))))
    8))

;; bytes schema

(defmethod serialized-size ((object vector))
  (let ((length (length object)))
    (+ (serialized-size length)
       length)))

(define-serialize-into vector
  "Write byte vector."
  `(progn
     (check-type object schema:bytes)
     (let ((bytes-written
             ,(if vectorp
                  '(serialize-into-vector (length object) vector start)
                  '(serialize-into-stream (length object) stream))))
       (declare (fixnum bytes-written))
       ,(if vectorp
            '(replace vector object :start1 (+ start bytes-written))
            '(write-sequence object stream))
       (the fixnum (+ bytes-written (length object))))))

(define-deserialize-from (eql 'schema:bytes)
  "Read a vector of bytes."
  (declare (ignore schema))
  `(multiple-value-bind (size bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     ,@(when vectorp
         `((unless (>= (- (length vector) start bytes-read) size)
              (error "Not enough bytes in vector"))))
     (let ((bytes (make-array size :element-type '(unsigned-byte 8))))
       ,(if vectorp
            ;; would returning a displaced array be better than copying?
            `(replace bytes vector :start2 (+ start bytes-read))
            `(unless (= (read-sequence bytes stream) size)
               (error 'end-of-file :stream *error-output*)))
       (values bytes (+ bytes-read size)))))

;; string schema

(defmethod serialized-size ((object string))
  (let ((length (babel:string-size-in-octets object :encoding :utf-8)))
    (+ (serialized-size length)
       length)))

(define-serialize-into string
  "Write utf-8 encoded string."
  (declare (inline babel:string-to-octets))
  `(let ((bytes (babel:string-to-octets object :encoding :utf-8)))
     ,(if vectorp
          '(serialize-into-vector bytes vector start)
          '(serialize-into-stream bytes stream))))

(define-deserialize-from (eql 'schema:string)
  "Read a utf-8 encoded string."
  (declare (ignore schema)
           (inline babel:octets-to-string))
  `(multiple-value-bind (size bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     ,@(when vectorp
         `((unless (>= (- (length vector) start bytes-read) size)
             (error "Not enough bytes in vector"))))
     (values
      (babel:octets-to-string
       ,(if vectorp
            'vector
            `(let ((bytes (make-array size :element-type '(unsigned-byte 8))))
               (unless (= (read-sequence bytes stream) size)
                 (error 'end-of-file :stream *error-output*))
               bytes))
       :encoding :utf-8
       ,@(when vectorp
           '(:start (+ start bytes-read) :end (+ start bytes-read size))))
      (+ bytes-read size))))
