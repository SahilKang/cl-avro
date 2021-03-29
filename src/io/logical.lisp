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
(defpackage #:cl-avro.io.logical
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:endian #:cl-avro.io.primitive))
  (:import-from #:cl-avro.io.base
                #:serialize-into
                #:serialized-size
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from)
  (:export #:serialize-into
           #:serialized-size
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.io.logical)

;;; uuid schema

(defmethod serialized-size ((object schema:uuid))
  (serialized-size (schema:uuid object)))

(defmethod serialize-into
    ((object schema:uuid) (vector simple-array) (start fixnum))
  "Write uuid string into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (schema:uuid object) vector start))

;; Read a uuid string from STREAM.
(define-deserialize-from schema:uuid-schema
  `(multiple-value-bind (uuid bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:string vector start)
            `(deserialize-from-stream 'schema:string stream))
     (values (make-instance schema :uuid uuid) bytes-read)))

;; TODO add symbol/class-name specializations (eql 'schema:uuid)

;;; date schema

(declaim
 (ftype (function (schema:date) (values schema:int &optional)) process-date)
 (inline process-date))
(defun process-date (date)
  (declare (optimize (speed 3) (safety 0)))
  (let ((unix-time (local-time:timestamp-to-unix date)))
    (declare (integer unix-time))
    (nth-value 0 (truncate unix-time (* 60 60 24)))))
(declaim (notinline process-date))

(defmethod serialized-size ((object schema:date))
  (serialized-size (process-date object)))

(defmethod serialize-into
    ((object schema:date) (vector simple-array) (start fixnum))
  "Write date into VECTOR.

Serialized as the number of days from the ISO unix epoch 1970-01-01."
  (declare (optimize (speed 3) (safety 0))
           (inline process-date)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-date object) vector start))

;; Read a date from STREAM.
(define-deserialize-from schema:date-schema
  `(multiple-value-bind (days bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:int vector start)
            `(deserialize-from-stream 'schema:int stream))
     (let ((timestamp
             (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
               (offset :day days))))
       (values (change-class timestamp schema) bytes-read))))

;;; time-millis schema

(declaim
 (ftype (function (schema:time-millis) (values schema:int &optional))
        process-time-millis)
 (inline process-time-millis))
(defun process-time-millis (time-millis)
  (declare (optimize (speed 3) (safety 0)))
  (local-time:with-decoded-timestamp
      (:hour hour :minute minute :sec second :nsec nanosecond)
      time-millis
    (+ (* hour 60 60 1000)
       (* minute 60 1000)
       (* second 1000)
       (truncate nanosecond (* 1000 1000)))))
(declaim (notinline process-time-millis))

(defmethod serialized-size ((object schema:time-millis))
  (serialized-size (process-time-millis object)))

(defmethod serialize-into
    ((object schema:time-millis) (vector simple-array) (start fixnum))
  "Write time of day into VECTOR.

Serialized as the number of milliseconds after midnight, 00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-time-millis)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-time-millis object) vector start))

;; Read time of day from STREAM.
(define-deserialize-from schema:time-millis-schema
  `(multiple-value-bind (milliseconds-after-midnight bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:int vector start)
            `(deserialize-from-stream 'schema:int stream))
     (declare ((and (integer 0) schema:int) milliseconds-after-midnight))
     (let ((hour (multiple-value-bind (hour remainder)
                     (truncate milliseconds-after-midnight (* 60 60 1000))
                   (setf milliseconds-after-midnight remainder)
                   hour))
           (minute (multiple-value-bind (minute remainder)
                       (truncate milliseconds-after-midnight (* 60 1000))
                     (setf milliseconds-after-midnight remainder)
                     minute)))
       (values
        (make-instance schema :hour hour :minute minute
                              :millisecond milliseconds-after-midnight)
        bytes-read))))

;;; time-micros schema

(declaim
 (ftype (function (schema:time-micros) (values schema:long &optional))
        process-time-micros)
 (inline process-time-micros))
(defun process-time-micros (time-micros)
  (declare (optimize (speed 3) (safety 0)))
  (local-time:with-decoded-timestamp
      (:hour hour :minute minute :sec second :nsec nanosecond)
      time-micros
    (+ (* hour 60 60 1000 1000)
       (* minute 60 1000 1000)
       (* second 1000 1000)
       (truncate nanosecond 1000))))
(declaim (notinline process-time-micros))

(defmethod serialized-size ((object schema:time-micros))
  (serialized-size (process-time-micros object)))

(defmethod serialize-into
    ((object schema:time-micros) (vector simple-array) (start fixnum))
  "Write time of day into VECTOR.

Serialized as the number of microseconds after midnight, 00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-time-micros)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-time-micros object) vector start))

;; Read time of day from STREAM.
(define-deserialize-from schema:time-micros-schema
  `(multiple-value-bind (microseconds-after-midnight bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (declare ((and (integer 0) schema:long) microseconds-after-midnight))
     (let ((hour (multiple-value-bind (hour remainder)
                     (truncate microseconds-after-midnight (* 60 60 1000 1000))
                   (setf microseconds-after-midnight remainder)
                   hour))
           (minute (multiple-value-bind (minute remainder)
                       (truncate microseconds-after-midnight (* 60 1000 1000))
                     (setf microseconds-after-midnight remainder)
                     minute)))
       (values
        (make-instance schema :hour hour :minute minute
                              :microsecond microseconds-after-midnight)
        bytes-read))))

;;; timestamp-millis schema

(declaim
 (ftype (function (schema:timestamp-millis) (values schema:long &optional))
        process-timestamp-millis)
 (inline process-timestamp-millis))
(defun process-timestamp-millis (timestamp-millis)
  (declare (optimize (speed 3) (safety 0)))
  (let ((seconds-from-unix-epoch (local-time:timestamp-to-unix timestamp-millis))
        (nanoseconds (local-time:nsec-of timestamp-millis)))
    (declare ((integer -9223372036854776 9223372036854775) seconds-from-unix-epoch)
             ((integer 0 999999999) nanoseconds))
    (+ (* 1000 seconds-from-unix-epoch)
       (truncate nanoseconds (* 1000 1000)))))
(declaim (notinline process-timestamp-millis))

(defmethod serialized-size ((object schema:timestamp-millis))
  (serialized-size (process-timestamp-millis object)))

(defmethod serialize-into
    ((object schema:timestamp-millis) (vector simple-array) (start fixnum))
  "Write timestamp into VECTOR.

Serialized as the number of milliseconds from the UTC unix epoch 1970-01-01T00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-timestamp-millis)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-timestamp-millis object) vector start))

;; Read a timestamp from STREAM.
(define-deserialize-from schema:timestamp-millis-schema
  `(multiple-value-bind (milliseconds-from-unix-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (declare (schema:long milliseconds-from-unix-epoch))
     (let* ((seconds-from-unix-epoch
              (multiple-value-bind (seconds remainder)
                  (truncate milliseconds-from-unix-epoch 1000)
                (setf milliseconds-from-unix-epoch remainder)
                seconds))
            (nanoseconds-from-unix-epoch
              (* milliseconds-from-unix-epoch 1000 1000))
            (timestamp
              (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
                (offset :sec seconds-from-unix-epoch)
                (offset :nsec nanoseconds-from-unix-epoch))))
       (values (change-class timestamp schema) bytes-read))))

;;; timestamp-micros schema

(declaim
 (ftype (function (schema:timestamp-micros) (values schema:long &optional))
        process-timestamp-micros)
 (inline process-timestamp-micros))
(defun process-timestamp-micros (timestamp-micros)
  (declare (optimize (speed 3) (safety 0)))
  (let ((seconds-from-unix-epoch (local-time:timestamp-to-unix timestamp-micros))
        (nanoseconds (local-time:nsec-of timestamp-micros)))
    (declare ((integer -9223372036855 9223372036854) seconds-from-unix-epoch)
             ((integer 0 999999999) nanoseconds))
    (+ (* 1000 1000 seconds-from-unix-epoch)
       (truncate nanoseconds 1000))))
(declaim (notinline process-timestamp-micros))

(defmethod serialized-size ((object schema:timestamp-micros))
  (serialized-size (process-timestamp-micros object)))

(defmethod serialize-into
    ((object schema:timestamp-micros) (vector simple-array) (start fixnum))
  "Write timestamp into VECTOR.

Serialized as the number of microseconds from the UTC unix epoch 1970-01-01T00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-timestamp-micros)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-timestamp-micros object) vector start))

;; Read timestamp from STREAM.
(define-deserialize-from schema:timestamp-micros-schema
  `(multiple-value-bind (microseconds-from-unix-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (declare (schema:long microseconds-from-unix-epoch))
     (let* ((seconds-from-unix-epoch
              (multiple-value-bind (seconds remainder)
                  (truncate microseconds-from-unix-epoch (* 1000 1000))
                (setf microseconds-from-unix-epoch remainder)
                seconds))
            (nanoseconds-from-unix-epoch
              (* microseconds-from-unix-epoch 1000))
            (timestamp
              (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
                (offset :sec seconds-from-unix-epoch)
                (offset :nsec nanoseconds-from-unix-epoch))))
       (values (change-class timestamp schema) bytes-read))))

;;; local-timestamp-millis schema

(declaim
 (ftype (function (schema:local-timestamp-millis)
                  (values schema:long &optional))
        process-local-timestamp-millis)
 (inline process-local-timestamp-millis))
(defun process-local-timestamp-millis (local-timestamp-millis)
  (declare (optimize (speed 3) (safety 0)))
  (let* ((epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970))
         (seconds-from-epoch (local-time:timestamp-difference
                              epoch local-timestamp-millis)))
    (declare ((or integer double-float) seconds-from-epoch))
    (nth-value 0 (truncate (* seconds-from-epoch 1000)))))
(declaim (notinline process-local-timestamp-millis))

(defmethod serialized-size ((object schema:local-timestamp-millis))
  (serialized-size (process-local-timestamp-millis object)))

(defmethod serialize-into
    ((object schema:local-timestamp-millis) (vector simple-array) (start fixnum))
  "Write local timestamp into VECTOR.

Serialized as the number of milliseconds from 1970-01-01T00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-local-timestamp-millis)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-local-timestamp-millis object) vector start))

;; Read local timestamp from STREAM.
(define-deserialize-from schema:local-timestamp-millis-schema
  `(multiple-value-bind (milliseconds-from-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (declare (schema:long milliseconds-from-epoch))
     (let* ((seconds-from-epoch
              (multiple-value-bind (seconds remainder)
                  (truncate milliseconds-from-epoch 1000)
                (setf milliseconds-from-epoch remainder)
                seconds))
            (nanoseconds-from-epoch
              (* milliseconds-from-epoch 1000 1000))
            (epoch
              (local-time:encode-timestamp 0 0 0 0 1 1 1970))
            (timestamp
              (local-time:adjust-timestamp! epoch
                (offset :sec seconds-from-epoch)
                (offset :nsec nanoseconds-from-epoch))))
       (values (change-class timestamp schema) bytes-read))))

;;; local-timestamp-micros schema

(declaim
 (ftype (function (schema:local-timestamp-micros)
                  (values schema:long &optional))
        process-local-timestamp-micros)
 (inline process-local-timestamp-micros))
(defun process-local-timestamp-micros (local-timestamp-micros)
  (declare (optimize (speed 3) (safety 0)))
  (let* ((epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970))
         (seconds-from-epoch (local-time:timestamp-difference
                              epoch local-timestamp-micros)))
    (declare ((or integer double-float) seconds-from-epoch))
    (nth-value 0 (truncate (* seconds-from-epoch 1000 1000)))))
(declaim (notinline process-local-timestamp-micros))

(defmethod serialized-size ((object schema:local-timestamp-micros))
  (serialized-size (process-local-timestamp-micros object)))

(defmethod serialize-into
    ((object schema:local-timestamp-micros) (vector simple-array) (start fixnum))
  "Write local timestamp into VECTOR.

Serialized as the number of microseconds from 1970-01-01T00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-local-timestamp-micros)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (serialize-into (process-local-timestamp-micros object) vector start))

;; Read local timestamp from STREAM.
(define-deserialize-from schema:local-timestamp-micros-schema
  `(multiple-value-bind (microseconds-from-epoch bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:long vector start)
            `(deserialize-from-stream 'schema:long stream))
     (declare (schema:long microseconds-from-epoch))
     (let* ((seconds-from-epoch
              (multiple-value-bind (seconds remainder)
                  (truncate microseconds-from-epoch (* 1000 1000))
                (setf microseconds-from-epoch remainder)
                seconds))
            (nanoseconds-from-epoch
              (* microseconds-from-epoch 1000))
            (epoch
              (local-time:encode-timestamp 0 0 0 0 1 1 1970))
            (timestamp
              (local-time:adjust-timestamp! epoch
                (offset :sec seconds-from-epoch)
                (offset :nsec nanoseconds-from-epoch))))
       (values (change-class timestamp schema) bytes-read))))

;;; duration schema

(defmethod serialized-size ((object schema:duration-object))
  12)

(defmethod serialize-into
    ((object schema:duration-object) (vector simple-array) (start fixnum))
  "Write duration into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           (inline endian:uint32->little-endian)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (endian:uint32->little-endian (schema:months object) vector start)
  (endian:uint32->little-endian (schema:days object) vector (+ start 4))
  (endian:uint32->little-endian (schema:milliseconds object) vector (+ start 8))
  12)

;; Read duration from STREAM.
(define-deserialize-from schema:duration
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

;; TODO not sure about this (integer 0) type

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values (integer 0) &optional))
        big-endian->integer)
 (inline big-endian->integer))
(defun big-endian->integer (bytes)
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with value of-type (integer 0) = 0

    for offset from (1- (length bytes)) downto 0
    for byte of-type (unsigned-byte 8) = (elt bytes offset)

    do (setf value (logior value (ash byte (* offset 8))))

    finally
       (return value)))
(declaim (notinline big-endian->integer))

(declaim
 (ftype (function
         ((integer 0) (simple-array (unsigned-byte 8) (*)) fixnum fixnum)
         (values &optional))
        integer->big-endian)
 (inline integer->big-endian))
(defun integer->big-endian (integer bytes start end)
  (declare (optimize (speed 3) (safety 0)))
  (loop
    for offset from (1- end) downto start
    for shift = (* 8 (the fixnum (- offset start)))
    for byte of-type (unsigned-byte 8) = (logand #xff (ash integer (- shift)))
    do (setf (elt bytes offset) byte)

    finally
       (return (values))))
(declaim (notinline integer->big-endian))

;; twos-complement

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values integer &optional))
        read-twos-complement)
 (inline read-twos-complement))
(defun read-twos-complement (bytes)
  (declare (optimize (speed 3) (safety 0))
           (inline big-endian->integer))
  (let* ((bits (* (length bytes) 8))
         (mask (ash 2 (1- bits)))
         (value (big-endian->integer bytes)))
    (+ (- (logand value mask))
       (logand value (lognot mask)))))
(declaim (notinline read-twos-complement))

(declaim
 (ftype (function (integer (simple-array (unsigned-byte 8) (*)) fixnum fixnum)
                  (values &optional))
        write-twos-complement)
 (inline write-twos-complement))
(defun write-twos-complement (integer bytes start end)
  (declare (optimize (speed 3) (safety 0))
           (inline integer->big-endian))
  (let ((value (if (minusp integer)
                   (1+ (lognot (abs integer)))
                   integer)))
    (integer->big-endian value bytes start end)))
(declaim (notinline write-twos-complement))

;; serialize

(declaim
 (ftype (function (integer (or (eql schema:bytes) schema:fixed))
                  (values (integer 0) &optional))
        min-buf-length)
 (inline min-buf-length))
(defun min-buf-length (unscaled schema)
  (declare (optimize (speed 3) (safety 0)))
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

(defmethod serialize-into
    ((object schema:decimal-object) (vector simple-array) (start fixnum))
  "Write decimal into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           (inline min-buf-length write-twos-complement)
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let* ((underlying (schema:underlying (class-of object)))
         (unscaled (schema:unscaled object))
         (min-length (min-buf-length unscaled underlying)))
    (if (eq underlying 'schema:bytes)
        (let* ((bytes-written (serialize-into min-length vector start))
               (new-start (+ start bytes-written))
               (end (+ new-start min-length)))
          (declare (fixnum bytes-written new-start end))
          (write-twos-complement unscaled vector new-start end)
          (the fixnum (- end start)))
        (let ((end (+ start min-length)))
          (declare (fixnum end))
          (write-twos-complement unscaled vector start end)
          (the fixnum (- end start))))))

;; deserialize

;; Read decimal from STREAM.
(define-deserialize-from schema:decimal
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
