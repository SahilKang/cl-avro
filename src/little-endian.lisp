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
(defpackage #:cl-avro.internal.little-endian
  (:use #:cl)
  (:import-from #:cl-avro.internal.type
                #:vector<uint8>
                #:ufixnum
                #:uint8)
  (:export #:vector->uint32
           #:vector->uint64
           #:stream->uint32
           #:stream->uint64
           #:uint32->vector
           #:uint64->vector
           #:uint32->stream
           #:uint64->stream))
(in-package #:cl-avro.internal.little-endian)

;;; to-uint

(defmacro define-to-uint (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "TO-UINT~A" bits)))
        (args (loop
                for i from 1 to (/ bits 8)
                collect (intern (format nil "BYTE~A" i))))
        (arg-types (loop repeat (/ bits 8) collect 'uint8))
        (uintN `(unsigned-byte ,bits)))
    `(progn
       (declaim (ftype (function ,arg-types (values ,uintN &optional)) ,name))
       (defun ,name ,args
         ,(let ((shifts (loop
                          for byte in (rest args)
                          for shift from 8 below bits by 8
                          collect `(ash ,byte ,shift))))
            (reduce (lambda (agg next)
                      `(logior ,agg ,next))
                    shifts
                    :initial-value (first args)))))))

(define-to-uint 32)

(define-to-uint 64)

;;; vector->uint

(defmacro define-vector->uint (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "VECTOR->UINT~A" bits)))
        (uintN `(unsigned-byte ,bits))
        (to-uint (intern (format nil "TO-UINT~A" bits))))
    `(progn
       (declaim
        (ftype (function (vector<uint8> ufixnum) (values ,uintN &optional)) ,name))
       (defun ,name (vector start)
         (,to-uint ,@(loop
                       for i below (/ bits 8)
                       for index = 'start then `(+ start ,i)
                       collect `(elt vector ,index)))))))

(define-vector->uint 32)

(define-vector->uint 64)

;;; stream->uint

(defmacro define-stream->uint (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "STREAM->UINT~A" bits)))
        (uintN `(unsigned-byte ,bits))
        (to-uint (intern (format nil "TO-UINT~A" bits)))
        (bytes (loop
                 for i from 1 to (/ bits 8)
                 collect (intern (format nil "BYTE~A" i)))))
    `(progn
       (declaim (ftype (function (stream) (values ,uintN &optional)) ,name))
       (defun ,name (stream)
         (let ,(mapcar (lambda (byte)
                         `(,byte (read-byte stream)))
                bytes)
           (declare (uint8 ,@bytes))
           (,to-uint ,@bytes))))))

(define-stream->uint 32)

(define-stream->uint 64)

;;; from-uint

(defmacro define-from-uint (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "FROM-UINT~A" bits)))
        (uintN `(unsigned-byte ,bits))
        (return-type
          `(values ,@(loop repeat (/ bits 8) collect 'uint8) &optional)))
    `(progn
       (declaim (ftype (function (,uintN) ,return-type) ,name))
       (defun ,name (integer)
         (values
          ,@(loop
              for shift from -8 above (* -1 bits) by 8
              collect `(logand #xff (ash integer ,shift)) into shifts
              finally
                 (return (cons '(logand #xff integer) shifts))))))))

(define-from-uint 32)

(define-from-uint 64)

;;; uint->vector/stream

(defmacro define-uint->vector/stream (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "UINT~A->~A" bits vector/stream)))
         (uintN `(unsigned-byte ,bits))
         (from-uint (intern (format nil "FROM-UINT~A" bits)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (bytes (loop
                  for i from 1 to (/ bits 8)
                  collect (intern (format nil "BYTE~A" i))))
         (body (if vectorp
                   (loop
                     for i below (/ bits 8)
                     for byte in bytes
                     for index = 'start then `(+ start ,i)
                     collect `(setf (elt vector ,index) ,byte))
                   (mapcar (lambda (byte)
                             `(write-byte ,byte stream))
                           bytes))))
    `(progn
       (declaim (ftype (function (,uintN ,@arg-types) (values &optional)) ,name))
       (defun ,name (integer ,@args)
         (multiple-value-bind ,bytes
             (,from-uint integer)
           ,@body)
         (values)))))

(define-uint->vector/stream 32 vector)
(define-uint->vector/stream 64 vector)

(define-uint->vector/stream 32 stream)
(define-uint->vector/stream 64 stream)
