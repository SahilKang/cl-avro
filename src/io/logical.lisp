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
                #:serialize
                #:deserialize)
  (:export #:serialize
           #:deserialize))
(in-package #:cl-avro.io.logical)

;;; uuid schema

(defmethod serialize ((object schema:uuid) &key stream)
  "Write uuid string into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (serialize (schema:uuid object) :stream stream)
  (values))

(defmethod deserialize ((schema schema:uuid-schema) (stream stream) &key)
  "Read a uuid string from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((uuid (deserialize 'schema:string stream)))
    (make-instance schema :uuid uuid)))

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

(defmethod serialize ((object schema:date) &key stream)
  "Write date into STREAM.

Serialized as the number of days from the ISO unix epoch 1970-01-01."
  (declare (optimize (speed 3) (safety 0))
           (inline process-date))
  (serialize (process-date object) :stream stream)
  (values))

(defmethod deserialize ((schema schema:date-schema) (stream stream) &key)
  "Read a date from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (change-class
   (let ((days (deserialize 'schema:int stream)))
     (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
       (offset :day days)))
   schema))

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

(defmethod serialize ((object schema:time-millis) &key stream)
  "Write time of day into STREAM.

Serialized as the number of milliseconds after midnight, 00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-time-millis))
  (serialize (process-time-millis object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:time-millis-schema) (stream stream) &key)
  "Read time of day from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((milliseconds-after-midnight (deserialize 'schema:int stream))
         (hour (multiple-value-bind (hour remainder)
                   (truncate milliseconds-after-midnight (* 60 60 1000))
                 (setf milliseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate milliseconds-after-midnight (* 60 1000))
                   (setf milliseconds-after-midnight remainder)
                   minute)))
    (declare ((and (integer 0) schema:int) milliseconds-after-midnight))
    (make-instance schema :hour hour :minute minute
                          :millisecond milliseconds-after-midnight)))

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

(defmethod serialize ((object schema:time-micros) &key stream)
  "Write time of day into STREAM.

Serialized as the number of microseconds after midnight, 00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-time-micros))
  (serialize (process-time-micros object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:time-micros-schema) (stream stream) &key)
  "Read time of day from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((microseconds-after-midnight (deserialize 'schema:long stream))
         (hour (multiple-value-bind (hour remainder)
                   (truncate microseconds-after-midnight (* 60 60 1000 1000))
                 (setf microseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate microseconds-after-midnight (* 60 1000 1000))
                   (setf microseconds-after-midnight remainder)
                   minute)))
    (declare ((and (integer 0) schema:long) microseconds-after-midnight))
    (make-instance schema :hour hour :minute minute
                   :microsecond microseconds-after-midnight)))

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

(defmethod serialize ((object schema:timestamp-millis) &key stream)
  "Write timestamp into STREAM.

Serialized as the number of milliseconds from the UTC unix epoch 1970-01-01T00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-timestamp-millis))
  (serialize (process-timestamp-millis object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:timestamp-millis-schema) (stream stream) &key)
  "Read a timestamp from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (change-class
   (let* ((milliseconds-from-unix-epoch
            (deserialize 'schema:long stream))
          (seconds-from-unix-epoch
            (multiple-value-bind (seconds remainder)
                (truncate milliseconds-from-unix-epoch 1000)
              (setf milliseconds-from-unix-epoch remainder)
              seconds))
          (nanoseconds-from-unix-epoch
            (* milliseconds-from-unix-epoch 1000 1000)))
     (declare (schema:long milliseconds-from-unix-epoch))
     (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
       (offset :sec seconds-from-unix-epoch)
       (offset :nsec nanoseconds-from-unix-epoch)))
   schema))

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

(defmethod serialize ((object schema:timestamp-micros) &key stream)
  "Write timestamp into STREAM.

Serialized as the number of microseconds from the UTC unix epoch 1970-01-01T00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-timestamp-micros))
  (serialize (process-timestamp-micros object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:timestamp-micros-schema) (stream stream) &key)
  "Read timestamp from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (change-class
   (let* ((microseconds-from-unix-epoch
            (deserialize 'schema:long stream))
          (seconds-from-unix-epoch
            (multiple-value-bind (seconds remainder)
                (truncate microseconds-from-unix-epoch (* 1000 1000))
              (setf microseconds-from-unix-epoch remainder)
              seconds))
          (nanoseconds-from-unix-epoch
            (* microseconds-from-unix-epoch 1000)))
     (declare (schema:long microseconds-from-unix-epoch))
     (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
       (offset :sec seconds-from-unix-epoch)
       (offset :nsec nanoseconds-from-unix-epoch)))
   schema))

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

(defmethod serialize ((object schema:local-timestamp-millis) &key stream)
  "Write local timestamp into STREAM.

Serialized as the number of milliseconds from 1970-01-01T00:00:00.000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-local-timestamp-millis))
  (serialize (process-local-timestamp-millis object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:local-timestamp-millis-schema) (stream stream) &key)
  "Read local timestamp from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (change-class
   (let* ((milliseconds-from-epoch
            (deserialize 'schema:long stream))
          (seconds-from-epoch
            (multiple-value-bind (seconds remainder)
                (truncate milliseconds-from-epoch 1000)
              (setf milliseconds-from-epoch remainder)
              seconds))
          (nanoseconds-from-epoch
            (* milliseconds-from-epoch 1000 1000))
          (epoch
            (local-time:encode-timestamp 0 0 0 0 1 1 1970)))
     (declare (schema:long milliseconds-from-epoch))
     (local-time:adjust-timestamp! epoch
       (offset :sec seconds-from-epoch)
       (offset :nsec nanoseconds-from-epoch)))
   schema))

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

(defmethod serialize ((object schema:local-timestamp-micros) &key stream)
  "Write local timestamp into STREAM.

Serialized as the number of microseconds from 1970-01-01T00:00:00.000000."
  (declare (optimize (speed 3) (safety 0))
           (inline process-local-timestamp-micros))
  (serialize (process-local-timestamp-micros object) :stream stream)
  (values))

(defmethod deserialize
    ((schema schema:local-timestamp-micros-schema) (stream stream) &key)
  "Read local timestamp from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (change-class
   (let* ((microseconds-from-epoch
            (deserialize 'schema:long stream))
          (seconds-from-epoch
            (multiple-value-bind (seconds remainder)
                (truncate microseconds-from-epoch (* 1000 1000))
              (setf microseconds-from-epoch remainder)
              seconds))
          (nanoseconds-from-epoch
            (* microseconds-from-epoch 1000))
          (epoch
            (local-time:encode-timestamp 0 0 0 0 1 1 1970)))
     (declare (schema:long microseconds-from-epoch))
     (local-time:adjust-timestamp! epoch
       (offset :sec seconds-from-epoch)
       (offset :nsec nanoseconds-from-epoch)))
   schema))

;;; duration schema

(defmethod serialize ((object schema:duration-object) &key stream)
  "Write duration into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline endian:uint32->little-endian))
  (let ((bytes (make-array 12 :element-type '(unsigned-byte 8))))
    (endian:uint32->little-endian (schema:months object) bytes 0)
    (endian:uint32->little-endian (schema:days object) bytes 4)
    (endian:uint32->little-endian (schema:milliseconds object) bytes 8)
    (write-sequence bytes stream))
  (values))

(defmethod deserialize ((schema schema:duration) (stream stream) &key)
  "Read duration from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline endian:little-endian->uint32))
  (let ((bytes (make-array 12 :element-type '(unsigned-byte 8))))
    (unless (= (read-sequence bytes stream) 12)
      (error 'end-of-file :stream *error-output*))
    (make-instance
     schema
     :months (endian:little-endian->uint32 bytes 0)
     :days (endian:little-endian->uint32 bytes 4)
     :milliseconds (endian:little-endian->uint32 bytes 8))))

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
 (ftype (function ((integer 0) (simple-array (unsigned-byte 8) (*)))
                  (values &optional))
        integer->big-endian)
 (inline integer->big-endian))
(defun integer->big-endian (integer bytes)
  (declare (optimize (speed 3) (safety 0)))
  (loop
    for offset from (1- (length bytes)) downto 0
    for byte of-type (unsigned-byte 8) = (logand #xff (ash integer
                                                           (- (* offset 8))))
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
 (ftype (function (integer (simple-array (unsigned-byte 8) (*)))
                  (values &optional))
        write-twos-complement)
 (inline write-twos-complement))
(defun write-twos-complement (integer bytes)
  (declare (optimize (speed 3) (safety 0))
           (inline integer->big-endian))
  (let ((value (if (minusp integer)
                   (1+ (lognot (abs integer)))
                   integer)))
    (integer->big-endian value bytes)))
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

(defmethod serialize ((object schema:decimal-object) &key stream)
  "Write decimal into STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline min-buf-length write-twos-complement))
  (let* ((underlying (schema:underlying (class-of object)))
         (unscaled (schema:unscaled object))
         (min-length (min-buf-length unscaled underlying))
         (bytes (make-array min-length :element-type '(unsigned-byte 8))))
    (write-twos-complement unscaled bytes)
    (if (eq underlying 'schema:bytes)
        (serialize bytes :stream stream)
        (write-sequence bytes stream)))
  (values))

;; deserialize

(defmethod deserialize ((schema schema:decimal) (stream stream) &key)
  "Read decimal from STREAM."
  (declare (optimize (speed 3) (safety 0))
           (inline read-twos-complement))
  (let* ((underlying (schema:underlying schema))
         (bytes (deserialize underlying stream))
         (unscaled (read-twos-complement
                    (if (eq 'schema:bytes underlying)
                        bytes
                        (schema:raw-buffer bytes)))))
    (make-instance schema :unscaled unscaled)))