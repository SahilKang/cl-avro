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
(defpackage #:cl-avro.io.logical
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:endian #:cl-avro.io.primitive))
  (:import-from #:cl-avro.io.base
                #:serialized-size
                #:serialize-into-vector
                #:serialize-into-stream
                #:define-serialize-into
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from)
  (:import-from #:cl-avro.io.underlying
                #:to-underlying
                #:from-underlying)
  (:export #:serialize-into-vector
           #:serialize-into-stream
           #:serialized-size
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.io.logical)

;;; uuid schema

(defmethod serialized-size ((object schema:uuid))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:uuid
  "Write uuid string."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:uuid-schema
  "Read a uuid string."
  `(multiple-value-bind (uuid bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:string vector start)
            `(deserialize-from-stream 'schema:string stream))
     (values (from-underlying schema uuid) bytes-read)))

;; TODO add symbol/class-name specializations (eql 'schema:uuid)

;;; date schema

(defmethod serialized-size ((object schema:date))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:date
  "Write date.

Serialized as the number of days from the ISO unix epoch 1970-01-01."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:date-schema
  "Read a date."
  `(multiple-value-bind (days bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:int vector start)
            `(deserialize-from-stream 'schema:int stream))
     (values (from-underlying schema days) bytes-read)))

;;; time-millis schema

(defmethod serialized-size ((object schema:time-millis))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:time-millis
  "Write time of day.

Serialized as the number of milliseconds after midnight, 00:00:00.000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:time-millis-schema
  "Read time of day."
  `(multiple-value-bind (milliseconds-after-midnight bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:int vector start)
            `(deserialize-from-stream 'schema:int stream))
     (values (from-underlying schema milliseconds-after-midnight) bytes-read)))

;;; time-micros schema

(defmethod serialized-size ((object schema:time-micros))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:time-micros
  "Write time of day.

Serialized as the number of microseconds after midnight, 00:00:00.000000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:time-micros-schema
  "Read time of day."
  `(multiple-value-bind (microseconds-after-midnight bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (values (from-underlying schema microseconds-after-midnight) bytes-read)))

;;; timestamp-millis schema

(defmethod serialized-size ((object schema:timestamp-millis))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:timestamp-millis
  "Write timestamp into VECTOR.

Serialized as the number of milliseconds from the UTC unix epoch 1970-01-01T00:00:00.000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:timestamp-millis-schema
  "Read a timestamp."
  `(multiple-value-bind (milliseconds-from-unix-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (values (from-underlying schema milliseconds-from-unix-epoch) bytes-read)))

;;; timestamp-micros schema

(defmethod serialized-size ((object schema:timestamp-micros))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:timestamp-micros
  "Write timestamp.

Serialized as the number of microseconds from the UTC unix epoch 1970-01-01T00:00:00.000000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:timestamp-micros-schema
  "Read timestamp."
  `(multiple-value-bind (microseconds-from-unix-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (values (from-underlying schema microseconds-from-unix-epoch) bytes-read)))

;;; local-timestamp-millis schema

(defmethod serialized-size ((object schema:local-timestamp-millis))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:local-timestamp-millis
  "Write local timestamp.

Serialized as the number of milliseconds from 1970-01-01T00:00:00.000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:local-timestamp-millis-schema
  "Read local timestamp."
  `(multiple-value-bind (milliseconds-from-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (values (from-underlying schema milliseconds-from-epoch) bytes-read)))

;;; local-timestamp-micros schema

(defmethod serialized-size ((object schema:local-timestamp-micros))
  (serialized-size (to-underlying object)))

(define-serialize-into schema:local-timestamp-micros
  "Write local timestamp.

Serialized as the number of microseconds from 1970-01-01T00:00:00.000000."
  (if vectorp
      '(serialize-into-vector (to-underlying object) vector start)
      '(serialize-into-stream (to-underlying object) stream)))

(define-deserialize-from schema:local-timestamp-micros-schema
  "Read local timestamp."
  `(multiple-value-bind (microseconds-from-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (values (from-underlying schema microseconds-from-epoch) bytes-read)))

;;; duration schema

(defmethod serialized-size ((object schema:duration-object))
  12)

(define-serialize-into schema:duration-object
  "Write duration."
  (declare (inline endian:uint32->little-endian))
  `(prog1 12
     ,@(if vectorp
           '((endian:uint32->little-endian (schema:months object) vector start)
             (endian:uint32->little-endian (schema:days object) vector (+ start 4))
             (endian:uint32->little-endian (schema:milliseconds object) vector (+ start 8)))
           '((endian:uint32->little-endian (schema:months object) stream)
             (endian:uint32->little-endian (schema:days object) stream)
             (endian:uint32->little-endian (schema:milliseconds object) stream)))))

(define-deserialize-from schema:duration
  "Read duration."
  (declare (inline endian:little-endian->uint32))
  `(let ((bytes ,(if vectorp
                     'vector
                     `(make-array 12 :element-type '(unsigned-byte 8))))
         ,@(when streamp '((start 0))))
     ,@(when streamp
         `((unless (= (read-sequence bytes stream) 12)
             (error 'end-of-file :stream *error-output*))))
     (values
      (make-instance
       schema
       :months (endian:little-endian->uint32 bytes start)
       :days (endian:little-endian->uint32 bytes (+ start 4))
       :milliseconds (endian:little-endian->uint32 bytes (+ start 8)))
      12)))

;;; decimal schema

;; big-endian

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values (integer 0) &optional))
        big-endian->integer)
 (inline big-endian->integer))
(defun big-endian->integer (bytes)
  (loop
    with value of-type (integer 0) = 0

    for offset below (length bytes)
    for byte of-type (unsigned-byte 8) = (elt bytes offset)
    for shift = (* 8 (1- (- (length bytes) offset))) then (- shift 8)

    do (setf value (logior value (ash byte shift)))

    finally
       (return value)))
(declaim (notinline big-endian->integer))

(defmacro to-big-endian (integer into length &optional (start nil startp))
  (declare (symbol integer into length start))
  `(loop
     repeat ,length
     for shift = (* 8 (1- ,length)) then (- shift 8)
     for byte of-type (unsigned-byte 8) = (logand #xff (ash ,integer (- shift)))
     ,@(when startp
         `(for offset from ,start))

     do ,(if startp
             `(setf (elt ,into offset) byte)
             `(write-byte byte ,into))

     finally
        (return (values))))

(declaim
 (ftype (function
         ((integer 0)
          (or stream (simple-array (unsigned-byte 8) (*)))
          fixnum
          &optional fixnum)
         (values &optional))
        integer->big-endian)
 (inline integer->big-endian))
(defun integer->big-endian (integer into length &optional (start 0))
  (etypecase into
    (stream
     (to-big-endian integer into length))
    ((simple-array (unsigned-byte 8) (*))
     (to-big-endian integer into length start))))
(declaim (notinline integer->big-endian))

;; twos-complement

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values integer &optional))
        read-twos-complement)
 (inline read-twos-complement))
(defun read-twos-complement (bytes)
  (declare (inline big-endian->integer))
  (let* ((bits (* (length bytes) 8))
         (mask (ash 1 (1- bits)))
         (value (big-endian->integer bytes)))
    (if (zerop (logand value mask))
        value
        (- value (expt 2 bits)))))
(declaim (notinline read-twos-complement))

(declaim
 (ftype (function
         (integer
          (or stream (simple-array (unsigned-byte 8) (*)))
          fixnum
          &optional fixnum)
         (values &optional))
        write-twos-complement)
 (inline write-twos-complement))
(defun write-twos-complement (integer into length &optional (start 0))
  (declare (inline integer->big-endian))
  (let ((value (if (minusp integer)
                   (let ((bits (* length 8)))
                     (+ integer (expt 2 bits)))
                   integer)))
    (integer->big-endian value into length start)))
(declaim (notinline write-twos-complement))

;; serialize

(declaim
 (ftype (function (integer (or (eql schema:bytes) schema:fixed))
                  (values (integer 0) &optional))
        min-buf-length)
 (inline min-buf-length))
(defun min-buf-length (unscaled schema)
  (if (eq schema 'schema:bytes)
      (nth-value 0 (ceiling (1+ (integer-length unscaled)) 8))
      (schema:size schema)))
(declaim (notinline min-buf-length))

(defmethod serialized-size ((object schema:decimal-object))
  (let* ((underlying (schema:underlying (class-of object)))
         (buf-length (min-buf-length (schema:unscaled object) underlying)))
    (if (eq underlying 'schema:bytes)
        (+ (serialized-size buf-length)
           buf-length)
        buf-length)))

(define-serialize-into schema:decimal-object
  "Write decimal."
  (declare (inline min-buf-length write-twos-complement))
  `(let* ((underlying (schema:underlying (class-of object)))
          (unscaled (schema:unscaled object))
          (min-length (min-buf-length unscaled underlying)))
     (if (eq underlying 'schema:bytes)
         (let ((bytes-written
                 ,(if vectorp
                      '(serialize-into-vector min-length vector start)
                      '(serialize-into-stream min-length stream))))
           (declare (fixnum bytes-written))
           ,(if vectorp
                '(write-twos-complement unscaled vector min-length (+ start bytes-written))
                '(write-twos-complement unscaled stream min-length))
           (+ min-length bytes-written))
         (prog1 min-length
           ,(if vectorp
                '(write-twos-complement unscaled vector min-length start)
                '(write-twos-complement unscaled stream min-length))))))

;; deserialize

(define-deserialize-from schema:decimal
  "Read decimal."
  (declare (inline read-twos-complement))
  `(let ((underlying (schema:underlying schema)))
     (multiple-value-bind (bytes bytes-read)
         ,(if vectorp
              `(deserialize-from-vector underlying vector start)
              `(deserialize-from-stream underlying stream))
       (let ((unscaled (read-twos-complement
                        (if (eq 'schema:bytes underlying)
                            bytes
                            (schema:raw-buffer bytes)))))
         (values (make-instance schema :unscaled unscaled) bytes-read)))))
