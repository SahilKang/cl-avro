;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
   (#:stream #:cl-avro.io.memory-stream)
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:serialize
                #:deserialize)
  (:export #:serialize
           #:deserialize
           #:little-endian->uint32
           #:uint32->little-endian))
(in-package #:cl-avro.io.primitive)

;; TODO be more permissive with the bytes
(defmethod serialize :around (object &rest initargs &key stream)
  (declare (optimize (speed 3) (safety 0)))
  (if (or stream (typep object 'schema:schema))
      (call-next-method)
      (let ((stream (make-instance 'stream:memory-output-stream)))
        (setf (getf initargs :stream) stream)
        (apply #'call-next-method object initargs)
        (let ((bytes (stream:bytes stream)))
          (declare ((vector (unsigned-byte 8)) bytes))
          (coerce bytes '(simple-array (unsigned-byte 8) (*))))
        ;; TODO make this return a simple-array...do the resizing manually
        #+nil
        (stream:bytes stream))))

(defmethod deserialize (schema (bytes simple-array) &key)
  (declare (optimize (speed 3) (safety 0)))
  (check-type bytes (simple-array (unsigned-byte 8) (*)))
  (let ((stream (make-instance 'stream:memory-input-stream :bytes bytes)))
    ;; would need to work with method combination since bytes is the
    ;; second arg
    #+nil(call-next-method schema stream)
    (deserialize schema stream)))

;; TODO do the same thing with serialize but with a handler-case
(defmethod deserialize ((schema symbol) input &key)
  (declare (optimize (speed 3) (safety 0)))
  (if (typep schema 'schema:primitive-schema)
      (call-next-method)
      (deserialize (find-class schema) input)))

;;; null schema

(defmethod serialize ((object null) &key stream)
  "Write zero bytes into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore object stream))
  (values))

(defmethod deserialize ((schema (eql 'schema:null)) (stream stream) &key)
  "Read zero bytes from STREAM and return nil."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema stream))
  nil)

;;; boolean schema

(defmethod serialize ((object (eql 'schema:true)) &key stream)
  "Write #o1 into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore object))
  (write-byte 1 stream)
  (values))

(defmethod serialize ((object (eql 'schema:false)) &key stream)
  "Write #o0 into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore object))
  (write-byte 0 stream)
  (values))

(defmethod deserialize ((schema (eql 'schema:boolean)) (stream stream) &key)
  "Read a byte from STREAM and return TRUE if it's 1, or FALSE if it's 0."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema))
  (ecase (read-byte stream)
    (0 'schema:false)
    (1 'schema:true)))

;;; int and long schemas

;; from zig-zag

(declaim
 (ftype (function ((unsigned-byte 32)) (values schema:int &optional))
        zig-zag->int)
 (inline zig-zag->int))
(defun zig-zag->int (zig-zag)
  (declare (optimize (speed 3) (safety 0)))
  (logxor (ash zig-zag -1) (- (logand zig-zag 1))))
(declaim (notinline zig-zag->int))

(declaim
 (ftype (function ((unsigned-byte 64)) (values schema:long &optional))
        zig-zag->long)
 (inline zig-zag->long))
(defun zig-zag->long (zig-zag)
  (declare (optimize (speed 3) (safety 0)))
  (logxor (ash zig-zag -1) (- (logand zig-zag 1))))
(declaim (notinline zig-zag->long))

;; to zig-zag

(declaim
 (ftype (function (schema:int) (values (unsigned-byte 32) &optional))
        int->zig-zag)
 (inline int->zig-zag))
(defun int->zig-zag (int)
  (declare (optimize (speed 3) (safety 0)))
  (logxor (ash int 1) (ash int -31)))
(declaim (notinline int->zig-zag))

(declaim
 (ftype (function (schema:long) (values (unsigned-byte 64) &optional))
        long->zig-zag)
 (inline long->zig-zag))
(defun long->zig-zag (long)
  (declare (optimize (speed 3) (safety 0)))
  (logxor (ash long 1) (ash long -63)))
(declaim (notinline long->zig-zag))

;; variable length integers

(defmacro read-variable-length-integer (stream bits)
  (declare (symbol stream)
           ((member 32 64) bits))
  (let* ((max-bytes (ceiling bits 7))
         (type (ecase bits (32 "int") (64 "long")))
         (error-message
           (format nil "Too many bytes for ~A, expected ~S bytes max"
                   type max-bytes)))
    `(loop
       with value of-type (unsigned-byte ,bits) = 0

       for offset of-type fixnum below ,max-bytes
       for byte of-type (unsigned-byte 8) = (read-byte ,stream)

       do (setf value (logior value (ash (logand byte #x7f)
                                         (* 7 offset))))

       when (zerop (logand byte #x80))
         return value

       finally
          (error ,error-message))))

(defmacro write-variable-length-integer (bits stream integer)
  (declare (symbol stream)
           ((member 32 64) bits))
  `(loop
     for integer of-type (unsigned-byte ,bits) = ,integer then (ash integer -7)
     until (zerop (logand integer (lognot #x7f)))
     for byte = (logior (logand integer #x7f) #x80)

     do (write-byte byte ,stream)

     finally
        (write-byte (logand integer #xff) ,stream)))

;; serialize

(defmethod serialize ((object integer) &key stream)
  "Write OBJECT into STREAM as a variable-length zig-zag integer."
  (declare (optimize (speed 3) (safety 0))
           (inline int->zig-zag long->zig-zag))
  (etypecase object
    (schema:int
     (write-variable-length-integer 32 stream (int->zig-zag object)))
    (schema:long
     (write-variable-length-integer 64 stream (long->zig-zag object))))
  (values))

;; deserialize int

(defmethod deserialize ((schema (eql 'schema:int)) (stream stream) &key)
  "Read a 32-bit variable-length zig-zag integer from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema)
           (inline zig-zag->int))
  (zig-zag->int (read-variable-length-integer stream 32)))

;; deserialize long

(defmethod deserialize ((schema (eql 'schema:long)) (stream stream) &key)
  "Read a 64-bit variable-length zig-zag integer from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema)
           (inline zig-zag->long))
  (zig-zag->long (read-variable-length-integer stream 64)))

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
  (declare (optimize (speed 3) (safety 0)))
  (from-little-endian 32 bytes start))
(declaim (notinline little-endian->uint32))

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)) &optional fixnum)
                  (values (unsigned-byte 64) &optional))
        little-endian->uint64)
 (inline little-endian->uint64))
(defun little-endian->uint64 (bytes &optional (start 0))
  (declare (optimize (speed 3) (safety 0)))
  (from-little-endian 64 bytes start))
(declaim (notinline little-endian->uint64))

;; to little-endian

(defmacro to-little-endian (bits integer bytes start)
  (declare ((member 32 64) bits)
           (symbol integer bytes start))
  (let ((offset (gensym))
        (byte (gensym))
        (index (gensym)))
    `(loop
       for ,offset of-type fixnum below ,(truncate bits 8)
       for ,byte of-type (unsigned-byte 8) = (logand #xff (ash ,integer
                                                               (* ,offset -8)))
       for ,index of-type fixnum = ,start then (1+ ,index)

       do (setf (elt ,bytes ,index) ,byte)

       finally
          (return (values)))))

(declaim
 (ftype (function ((unsigned-byte 32)
                   (simple-array (unsigned-byte 8) (*))
                   &optional fixnum)
                  (values &optional))
        uint32->little-endian)
 (inline uint32->little-endian))
(defun uint32->little-endian (integer bytes &optional (start 0))
  (declare (optimize (speed 3) (safety 0)))
  (to-little-endian 32 integer bytes start))
(declaim (notinline uint32->little-endian))

(declaim
 (ftype (function ((unsigned-byte 64)
                   (simple-array (unsigned-byte 8) (*))
                   &optional fixnum)
                  (values &optional))
        uint64->little-endian)
 (inline uint64->little-endian))
(defun uint64->little-endian (integer bytes &optional (start 0))
  (declare (optimize (speed 3) (safety 0)))
  (to-little-endian 64 integer bytes start))
(declaim (notinline uint64->little-endian))

;; float schema

(defmethod serialize ((object single-float) &key stream)
  "Write single-precision float to STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline uint32->little-endian ieee-floats:encode-float32))
  (let ((integer (ieee-floats:encode-float32 object))
        (bytes (make-array 4 :element-type '(unsigned-byte 8))))
    (uint32->little-endian integer bytes)
    (write-sequence bytes stream))
  (values))

(defmethod deserialize ((schema (eql 'schema:float)) (stream stream) &key)
  "Read a single-precision float from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema)
           (inline little-endian->uint32 ieee-floats:decode-float32))
  (let ((bytes (make-array 4 :element-type '(unsigned-byte 8))))
    (unless (= (read-sequence bytes stream) 4)
      (error 'end-of-file :stream *error-output*))
    (ieee-floats:decode-float32 (little-endian->uint32 bytes))))

;; double schema

(defmethod serialize ((object double-float) &key stream)
  "Write double-precision float to STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline uint64->little-endian ieee-floats:encode-float64))
  (let ((integer (ieee-floats:encode-float64 object))
        (bytes (make-array 8 :element-type '(unsigned-byte 8))))
    (uint64->little-endian integer bytes)
    (write-sequence bytes stream))
  (values))

(defmethod deserialize ((schema (eql 'schema:double)) (stream stream) &key)
  "Read a double-precision float from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema)
           (inline little-endian->uint64 ieee-floats:decode-float64))
  (let ((bytes (make-array 8 :element-type '(unsigned-byte 8))))
    (unless (= (read-sequence bytes stream) 8)
      (error 'end-of-file :stream *error-output*))
    (ieee-floats:decode-float64 (little-endian->uint64 bytes))))

;; bytes schema

(defmethod serialize ((object array) &key stream)
  "Write byte array into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (check-type object schema:bytes)
  (serialize (length object) :stream stream)
  (write-sequence object stream)
  (values))

(defmethod deserialize ((schema (eql 'schema:bytes)) (stream stream) &key)
  "Read an array of bytes from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (ignore schema))
  (let* ((size (deserialize 'schema:long stream))
         (bytes (make-array size :element-type '(unsigned-byte 8))))
    (unless (= (read-sequence bytes stream) size)
      (error 'end-of-file :stream *error-output*))
    bytes))

;; string schema

(defmethod serialize ((object string) &key stream)
  "Write utf-8 encoded string into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline babel:string-to-octets))
  (let ((bytes (babel:string-to-octets object :encoding :utf-8)))
    (serialize bytes :stream stream)))

(defmethod deserialize ((schema (eql 'schema:string)) (stream stream) &key)
  "Read a utf-8 encoded string from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline babel:octets-to-string))
  (let ((bytes (deserialize 'schema:bytes stream)))
    (babel:octets-to-string bytes :encoding :utf-8)))
