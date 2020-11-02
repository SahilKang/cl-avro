;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(in-package #:cl-avro)

;; TODO this should return a second value: number of bytes read
(defgeneric deserialize (reader-schema input &optional writer-schema)
  (:method (reader-schema (bytes simple-vector) &optional writer-schema)
    (declare (ignore writer-schema)
             (optimize (speed 3) (safety 0)))
    (let ((stream (make-instance 'byte-vector-input-stream :bytes bytes)))
      (deserialize reader-schema stream)))

  (:documentation
   "Deserialize next object from INPUT according to READER-SCHEMA.

If WRITER-SCHEMA is non-nil, then schema resolution is performed
before deserialization."))

;; TODO this should return a second value: number of bytes written
;; when stream is nil, then it should return only bytes written
(defgeneric serialize (schema object &optional stream)
  (:method :before (schema object &optional stream)
    (declare (ignore stream)
             (optimize (speed 3) (safety 0)))
    (assert-valid schema object))

  (:method :around (schema object &optional stream)
    (declare (inline to-simple-vector)
             (optimize (speed 3) (safety 0)))
    (if stream
        (call-next-method)
        (let ((stream (make-instance 'byte-vector-output-stream)))
          (call-next-method schema object stream)
          (to-simple-vector stream))))

  (:documentation
   "Serialize OBJECT into STREAM according to avro SCHEMA.

If STREAM is nil, then the serialized bytes are returned as a vector."))

;;; avro primitive schemas

;; null-schema

(defmethod deserialize ((schema (eql 'null-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read zero bytes from STREAM and return nil."
  (declare (ignore schema stream writer-schema)
           (optimize (speed 3) (safety 0)))
  nil)

(defmethod serialize ((schema (eql 'null-schema))
                      (nada null)
                      &optional stream)
  "Write zero bytes to STREAM."
  (declare (ignore schema nada stream)
           (optimize (speed 3) (safety 0)))
  nil)

;; boolean-schema

(defmethod deserialize ((schema (eql 'boolean-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a byte from STREAM and return nil if it's 0 or t if it's 1."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (ecase (read-byte stream)
    (0 nil)
    (1 t)))

(defmethod serialize ((schema (eql 'boolean-schema))
                      boolean
                      &optional stream)
  "Write byte 1 to STREAM if BOOLEAN is non-nil; otherwise, write byte 0."
  (declare (ignore schema)
           (boolean-schema boolean)
           (optimize (speed 3) (safety 0)))
  (write-byte (if boolean 1 0) stream))

;; int-schema

(defmacro read-zig-zag (n)
  (let ((nn (gensym)))
    `(let ((,nn ,n))
       (logxor (ash ,nn -1) (- (logand ,nn 1))))))

(defmacro write-zig-zag (n)
  (let ((nn (gensym)))
    `(let ((,nn ,n))
       (logxor (ash ,nn 1) (ash ,nn -63)))))

(defmacro read-variable-length-number (stream bits)
  (declare (symbol stream)
           ((member 32 64) bits))
  (let* ((max-bytes (truncate bits 7))
         (error-message
           (format nil "Too many bytes for number, expected ~S bytes max" max-bytes)))
    `(loop
       with value of-type (unsigned-byte ,bits) = 0

       for offset of-type fixnum below ,max-bytes

       for byte of-type (unsigned-byte 8) = (read-byte ,stream)
       do (setf value (logior value (ash (logand byte #x7f)
                                         (* 7 offset))))

       when (zerop (logand byte #x80))
         return value

       finally (error ,error-message))))

(defmacro write-variable-length-number (stream number)
  (declare (symbol stream))
  `(loop
     for number = ,number then (ash number -7)
     until (zerop (logand number (lognot #x7f)))
     for byte = (logior (logand number #x7f) #x80)

     do (write-byte byte ,stream)

     finally (write-byte (logand number #xff) ,stream)))

(defmethod deserialize ((schema (eql 'int-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a 32-bit variable-length zig-zag integer from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (read-zig-zag (read-variable-length-number stream 32)))

(defmethod serialize ((schema (eql 'int-schema))
                      (integer integer)
                      &optional stream)
  "Write 32-bit variable-length zig-zag INTEGER to STREAM."
  (declare (ignore schema)
           (int-schema integer)
           (optimize (speed 3) (safety 0)))
  (write-variable-length-number stream (write-zig-zag integer)))

;; long-schema

(defmethod deserialize ((schema (eql 'long-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a 64-bit variable-length zig-zag integer from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (read-zig-zag (read-variable-length-number stream 64)))

(defmethod serialize ((schema (eql 'long-schema))
                      (integer integer)
                      &optional stream)
  "Write 64-bit variable-length zig-zag INTEGER to STREAM."
  (declare (ignore schema)
           (long-schema integer)
           (optimize (speed 3) (safety 0)))
  (write-variable-length-number stream (write-zig-zag integer)))

;; float-schema

(defmacro read-little-endian (bytes bits &optional (start 0))
  "Read from BYTES starting at START."
  (declare (symbol bytes)
           ((member 32 64) bits)
           ((or int-schema symbol) start))
  (let ((type `(unsigned-byte ,bits)))
    `(loop
       with value of-type ,type = 0

       for offset below ,(truncate bits 8)
       ;; TODO should use gensyms for loop variables
       for index = ,start then (1+ index)
       for byte of-type (unsigned-byte 8) = (svref ,bytes index)

       do (setf value (logior value (ash byte (* offset 8))))

       finally (return value))))

(defmacro write-little-endian (number bits bytes &optional (start 0))
  "Write into BYTES starting at START and return BYTES."
  (declare (symbol bytes)
           ((member 32 64) bits)
           ((or int-schema symbol) start)
           ((or int-schema symbol cons) number))
  (let ((num (gensym)))
    `(loop
       with ,num = ,number

       for offset below ,(truncate bits 8)

       for byte = (logand #xff (ash ,num (* offset -8)))
       ;; TODO should use gensyms for loop variables
       for index = ,start then (1+ index)
       do (setf (svref ,bytes index) byte)

       finally (return ,bytes))))

(defunc ieee-function (encode/decode bits)
  (declare ((member encode decode) encode/decode)
           ((member 32 64) bits))
  (let ((name (format nil "~S-FLOAT~S" encode/decode bits)))
    (multiple-value-bind (function status)
        (find-symbol name 'ieee-floats)
      (unless (eq status :external)
        (error "~S is not external in package ~S" name 'ieee-floats))
      (unless (fboundp function)
        (error "~S:~S is not a function" 'ieee-floats name))
      function)))

(defmacro read-floating-point (stream bits)
  (declare (symbol stream)
           ((member 32 64) bits))
  (let ((decode-float (ieee-function 'decode bits))
        (size (truncate bits 8))
        (buf (gensym)))
    `(let ((,buf (make-array ,size)))
       (unless (= (read-sequence ,buf ,stream) ,size)
         (error 'end-of-file :stream *error-output*))
       (,decode-float (read-little-endian ,buf ,bits)))))

(defmacro write-floating-point (stream float bits)
  (declare (symbol stream float)
           ((member 32 64) bits))
  (let ((encode-float (ieee-function 'encode bits))
        (size (truncate bits 8))
        (number (gensym))
        (buf (gensym)))
    `(let ((,number (,encode-float ,float))
           (,buf (make-array ,size)))
       (write-little-endian ,number ,bits ,buf)
       (write-sequence ,buf ,stream))))

(defmethod deserialize ((schema (eql 'float-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a 32-bit float from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (read-floating-point stream 32))

(defmethod serialize ((schema (eql 'float-schema))
                      (float single-float)
                      &optional stream)
  "Write 32-bit FLOAT to STREAM."
  (declare (ignore schema)
           (float-schema float))
  (write-floating-point stream float 32))

;; double-schema

(defmethod deserialize ((schema (eql 'double-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a 64-bit float from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (read-floating-point stream 64))

(defmethod serialize ((schema (eql 'double-schema))
                      (double double-float)
                      &optional stream)
  "Write 64-bit DOUBLE to STREAM."
  (declare (ignore schema)
           (double-schema double))
  (write-floating-point stream double 64))

;; bytes-schema

(defmethod deserialize ((schema (eql 'bytes-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a vector of bytes from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((size (deserialize 'long-schema stream))
         (buf (make-array size)))
    (unless (= (read-sequence buf stream) size)
      (error 'end-of-file :stream *error-output*))
    buf))

(defmethod serialize ((schema (eql 'bytes-schema))
                      (bytes simple-vector)
                      &optional stream)
  "Write BYTES to STREAM."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (serialize 'long-schema (length bytes) stream)
  (write-sequence bytes stream))

;; string-schema

(defmethod deserialize ((schema (eql 'string-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a string from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (let ((bytes (deserialize 'bytes-schema stream)))
    (babel:octets-to-string bytes :encoding :utf-8)))

(defmethod serialize ((schema (eql 'string-schema))
                      (string simple-string)
                      &optional stream)
  "Write STRING to STREAM."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (let ((bytes (babel:string-to-octets string :encoding :utf-8)))
    (serialize 'bytes-schema bytes stream)))

;;; avro logical schemas aliasing primitives

;; uuid-schema

(defmethod deserialize ((schema (eql 'uuid-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a uuid string from STREAM."
  (declare (ignore schema writer-schema)
           (inline rfc-4122-uuid-p)
           (optimize (speed 3) (safety 0)))
  (let ((uuid (deserialize 'string-schema stream)))
    (unless (rfc-4122-uuid-p uuid)
      (cerror "Return ~S anyway."
              "~S is not a valid UUID according to RFC-4122"
              uuid))
    uuid))

(defmethod serialize ((schema (eql 'uuid-schema))
                      (uuid simple-string)
                      &optional stream)
  "Write UUID to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (serialize 'string-schema uuid stream))

;; date-schema

(defmethod deserialize ((schema (eql 'date-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a date local-time:timestamp from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (let ((days (deserialize 'int-schema stream)))
    (declare (date-schema days))
    (local-time:adjust-timestamp! (local-time:unix-to-timestamp 0)
      (offset :day days))))

(defmethod serialize ((schema (eql 'date-schema))
                      (date local-time:timestamp)
                      &optional stream)
  "Write DATE to STREAM."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (let ((days (truncate (the integer (local-time:timestamp-to-unix date))
                        #.(* 60 60 24))))
    (declare (date-schema days))
    (serialize 'int-schema days stream)))

;; time-millis-schema

(defmethod deserialize ((schema (eql 'time-millis-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a time-of-day local-time:timestamp from STREAM."
  (declare (ignore schema writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((milliseconds-after-midnight (deserialize 'int-schema stream))
         (hour (multiple-value-bind (hour remainder)
                   (truncate milliseconds-after-midnight #.(* 60 60 1000))
                 (setf milliseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate milliseconds-after-midnight #.(* 60 1000))
                   (setf milliseconds-after-midnight remainder)
                   minute))
         (second (multiple-value-bind (second remainder)
                     (truncate milliseconds-after-midnight 1000)
                   (setf milliseconds-after-midnight remainder)
                   second))
         (nanosecond (* milliseconds-after-midnight 1000 1000)))
    (declare (time-millis-schema milliseconds-after-midnight))
    (local-time:encode-timestamp nanosecond second minute hour 1 1 1970)))

(defmethod serialize ((schema (eql 'time-millis-schema))
                      (time-of-day local-time:timestamp)
                      &optional stream)
  "Write TIME-OF-DAY to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (local-time:with-decoded-timestamp
      (:hour hour :minute minute :sec second :nsec nanosecond)
      time-of-day
    (let ((milliseconds-after-midnight (+ (* hour 60 60 1000)
                                          (* minute 60 1000)
                                          (* second 1000)
                                          (truncate nanosecond #.(* 1000 1000)))))
      (declare (time-millis-schema milliseconds-after-midnight))
      (serialize 'int-schema milliseconds-after-midnight stream))))

;; time-micros-schema

(defmethod deserialize ((schema (eql 'time-micros-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a time-of-day local-time:timestamp from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((microseconds-after-midnight (deserialize 'long-schema stream))
         (hour (multiple-value-bind (hour remainder)
                   (truncate microseconds-after-midnight #.(* 60 60 1000 1000))
                 (setf microseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate microseconds-after-midnight #.(* 60 1000 1000))
                   (setf microseconds-after-midnight remainder)
                   minute))
         (second (multiple-value-bind (second remainder)
                     (truncate microseconds-after-midnight #.(* 1000 1000))
                   (setf microseconds-after-midnight remainder)
                   second))
         (nanosecond (* microseconds-after-midnight 1000)))
    (declare (time-micros-schema microseconds-after-midnight))
    (local-time:encode-timestamp nanosecond second minute hour 1 1 1970)))

(defmethod serialize ((schema (eql 'time-micros-schema))
                      (time-of-day local-time:timestamp)
                      &optional stream)
  "Write TIME-OF-DAY to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (local-time:with-decoded-timestamp
      (:hour hour :minute minute :sec second :nsec nanosecond)
      time-of-day
    (let ((microseconds-after-midnight (+ (* hour 60 60 1000 1000)
                                          (* minute 60 1000 1000)
                                          (* second 1000 1000)
                                          (truncate nanosecond 1000))))
      (declare (time-micros-schema microseconds-after-midnight))
      (serialize 'long-schema microseconds-after-midnight stream))))

;; timestamp-millis-schema

(defmethod deserialize ((schema (eql 'timestamp-millis-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a timestamp local-time:timestamp from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((milliseconds-from-unix-epoch (deserialize 'long-schema stream))
         (nanoseconds-from-unix-epoch (* milliseconds-from-unix-epoch 1000 1000))
         (unix-epoch (local-time:unix-to-timestamp 0)))
    (declare (timestamp-millis-schema milliseconds-from-unix-epoch))
    (local-time:adjust-timestamp! unix-epoch
      (offset :nsec nanoseconds-from-unix-epoch))))

(defmethod serialize ((schema (eql 'timestamp-millis-schema))
                      (timestamp local-time:timestamp)
                      &optional stream)
  "Write TIMESTAMP to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((milliseconds-from-unix-epoch
          (+ (* 1000 (local-time:timestamp-to-unix timestamp))
             (truncate (local-time:nsec-of timestamp) #.(* 1000 1000)))))
    (declare (timestamp-millis-schema milliseconds-from-unix-epoch))
    (serialize 'long-schema milliseconds-from-unix-epoch stream)))

;; timestamp-micros-schema

(defmethod deserialize ((schema (eql 'timestamp-micros-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a timestamp local-time:timestamp from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((microseconds-from-unix-epoch (deserialize 'long-schema stream))
         (nanoseconds-from-unix-epoch (* microseconds-from-unix-epoch 1000))
         (unix-epoch (local-time:unix-to-timestamp 0)))
    (declare (timestamp-micros-schema microseconds-from-unix-epoch))
    (local-time:adjust-timestamp! unix-epoch
      (offset :nsec nanoseconds-from-unix-epoch))))

(defmethod serialize ((schema (eql 'timestamp-micros-schema))
                      (timestamp local-time:timestamp)
                      &optional stream)
  "Write TIMESTAMP to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((microseconds-from-unix-epoch
          (+ (* 1000 1000 (local-time:timestamp-to-unix timestamp))
             (truncate (local-time:nsec-of timestamp) 1000))))
    (declare (timestamp-micros-schema microseconds-from-unix-epoch))
    (serialize 'long-schema microseconds-from-unix-epoch stream)))

;; local-timestamp-millis-schema

(defmethod deserialize ((schema (eql 'local-timestamp-millis-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a timestamp local-time:timestamp from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((milliseconds-from-epoch (deserialize 'long-schema stream))
         (nanoseconds-from-epoch (* milliseconds-from-epoch 1000 1000))
         (epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970)))
    (declare (local-timestamp-millis-schema milliseconds-from-epoch))
    (local-time:adjust-timestamp! epoch
      (offset :nsec nanoseconds-from-epoch))))

(defmethod serialize ((schema (eql 'local-timestamp-millis-schema))
                      (timestamp local-time:timestamp)
                      &optional stream)
  "Write TIMESTAMP to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970))
         (seconds-from-epoch (local-time:timestamp-difference epoch timestamp))
         (milliseconds-from-epoch (truncate (* 1000 seconds-from-epoch))))
    (declare (local-timestamp-millis-schema milliseconds-from-epoch))
    (serialize 'long-schema milliseconds-from-epoch stream)))

;; local-timestamp-micros-schema

(defmethod deserialize ((schema (eql 'local-timestamp-micros-schema))
                        (stream stream)
                        &optional writer-schema)
  "Read a timestamp local-time:timestamp from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((microseconds-from-epoch (deserialize 'long-schema stream))
         (nanoseconds-from-epoch (* microseconds-from-epoch 1000))
         (epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970)))
    (declare (local-timestamp-micros-schema microseconds-from-epoch))
    (local-time:adjust-timestamp! epoch
      (offset :nsec nanoseconds-from-epoch))))

(defmethod serialize ((schema (eql 'local-timestamp-micros-schema))
                      (timestamp local-time:timestamp)
                      &optional stream)
  "Write TIMESTAMP to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((epoch (local-time:encode-timestamp 0 0 0 0 1 1 1970))
         (seconds-from-epoch (local-time:timestamp-difference epoch timestamp))
         (microseconds-from-epoch (truncate (* 1000 1000 seconds-from-epoch))))
    (declare (local-timestamp-micros-schema microseconds-from-epoch))
    (serialize 'long-schema microseconds-from-epoch stream)))
