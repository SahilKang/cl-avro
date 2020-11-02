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

;;; avro complex schemas

;; fixed-schema

(defmethod deserialize ((schema fixed-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read fixed number of bytes from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((size (fixed-schema-size schema))
         (buf (make-array size)))
    (unless (= size (read-sequence buf stream))
      (error 'end-of-file :stream *error-output*))
    buf))

(defmethod serialize ((schema fixed-schema)
                      (bytes simple-vector)
                      &optional stream)
  "Write BYTES to STREAM."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (write-sequence bytes stream))

;; union-schema

(defmethod deserialize ((schema union-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a tagged-union from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((schemas (union-schema-schemas schema))
         (position (deserialize 'long-schema stream))
         (chosen-schema (svref schemas position))
         (value (deserialize chosen-schema stream)))
    (%make-tagged-union :value value :schema chosen-schema)))

(defmethod serialize ((schema union-schema)
                      (tagged-union tagged-union)
                      &optional stream)
  "Write TAGGED-UNION to STREAM."
  (declare (inline schema-key)
           (optimize (speed 3) (safety 0)))
  (let* ((chosen-schema (tagged-union-schema tagged-union))
         (value (tagged-union-value tagged-union))
         (key (schema-key chosen-schema))
         (schemas (union-schema-schemas schema))
         (position (position key schemas :test #'equal :key #'schema-key)))
    (serialize 'long-schema position stream)
    (serialize chosen-schema value stream)))

;; array-schema

(defmethod deserialize ((schema array-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a vector from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (loop
    with array-stream = (make-instance 'array-input-stream
                                       :schema (array-schema-items schema)
                                       :stream stream)

    and vector = (make-array 0 :adjustable t :fill-pointer 0)

    for item = (stream-read-item array-stream)
    until (eq item :eof)
    do (vector-push-extend item vector)

    finally (return (coerce vector 'simple-vector))))

(defmethod serialize ((schema array-schema)
                      (vector simple-vector)
                      &optional stream)
  "Write VECTOR to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((count (length vector))
        (item-schema (array-schema-items schema)))
    (unless (zerop count)
      (serialize 'long-schema count stream)
      (flet ((serialize (item)
               (serialize item-schema item stream)))
        (map nil #'serialize vector))))
  (serialize 'long-schema 0 stream))

;; map-schema

(defmethod deserialize ((schema map-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a hash-table from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (loop
    with map-stream = (make-instance 'map-input-stream
                                     :schema (map-schema-values schema)
                                     :stream stream)

    and hash-table = (make-hash-table :test #'equal)

    for pair = (stream-read-item map-stream)
    until (eq pair :eof)
    for (key . value) = pair
    do (setf (gethash key hash-table) value)

    finally (return hash-table)))

(defmethod serialize ((schema map-schema)
                      (hash-table hash-table)
                      &optional stream)
  "Write HASH-TABLE to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((count (hash-table-count hash-table))
        (value-schema (map-schema-values schema)))
    (unless (zerop count)
      (serialize 'long-schema count stream)
      (flet ((serialize (key value)
               (serialize 'string-schema key stream)
               (serialize value-schema value stream)))
        (maphash #'serialize hash-table))))
  (serialize 'long-schema 0 stream))

;; enum-schema

(defmethod deserialize ((schema enum-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a string from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let ((symbols (enum-schema-symbols schema))
        (position (deserialize 'int-schema stream)))
    (svref symbols position)))

(defmethod serialize ((schema enum-schema)
                      (string simple-string)
                      &optional stream)
  "Write STRING to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((symbols (enum-schema-symbols schema))
         (position (position string symbols :test #'string=)))
    (serialize 'int-schema position stream)))

;; record-schema

(defmethod deserialize ((schema record-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a hash-table from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (loop
    with fields = (record-schema-fields schema)
    with hash-table = (make-hash-table :test #'equal :size (length fields))

    for field across fields
    for name = (field-schema-name field)
    for type = (field-schema-type field)

    for value = (deserialize type stream)
    do (setf (gethash name hash-table) value)

    finally (return hash-table)))

(defmethod serialize ((schema record-schema)
                      (hash-table hash-table)
                      &optional stream)
  "Write HASH-TABLE to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with fields = (record-schema-fields schema)

    for field across fields
    for name = (field-schema-name field)
    for type = (field-schema-type field)

    for value = (gethash name hash-table)
    do (serialize type value stream)))

;;; avro logical schemas

;; duration-schema

(defmethod deserialize ((schema duration-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read a duration from STREAM."
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let* ((fixed-schema (duration-schema-underlying-schema schema))
         (bytes (deserialize fixed-schema stream)))
    (%make-duration
     :months (read-little-endian bytes 32 0)
     :days (read-little-endian bytes 32 4)
     :milliseconds (read-little-endian bytes 32 8))))

(defmethod serialize ((schema duration-schema)
                      (duration duration)
                      &optional stream)
  "Write DURATION to STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((fixed-schema (duration-schema-underlying-schema schema))
        (bytes (make-array 12)))
    (write-little-endian (duration-months duration) 32 bytes 0)
    (write-little-endian (duration-days duration) 32 bytes 4)
    (write-little-endian (duration-milliseconds duration) 32 bytes 8)
    (serialize fixed-schema bytes stream)))

;; decimal-schema

(defun! read-big-endian (bytes)
    ((vector[byte]) integer)
  (loop
    with value = 0

    for offset from (1- (length bytes)) downto 0
    for byte of-type (unsigned-byte 8) = (svref bytes offset)
    for bits = (* offset 8)

    do (setf value (logior value (ash byte bits)))

    finally (return value)))

(defun! write-big-endian (integer bytes)
    ((integer vector[byte]) vector[byte])
  (loop
    for offset from (1- (length bytes)) downto 0
    for bits = (* offset 8)
    for byte of-type (unsigned-byte 8) = (logand #xff (ash integer (- bits)))

    do (setf (svref bytes offset) byte)

    finally (return bytes)))

(defun! read-twos-complement (bytes)
    ((vector[byte]) integer)
  (declare (inline read-big-endian))
  (let* ((bits (* 8 (length bytes)))
         (mask (ash 2 (1- bits)))
         (value (read-big-endian bytes)))
    (+ (- (logand value mask))
       (logand value (lognot mask)))))

(defun! write-twos-complement (integer bytes)
    ((integer vector[byte]) vector[byte])
  (declare (inline write-big-endian))
  (let ((twos-complement (if (minusp integer)
                             (1+ (lognot (abs integer)))
                             integer)))
    (write-big-endian twos-complement bytes)))

(defmethod deserialize ((schema decimal-schema)
                        (stream stream)
                        &optional writer-schema)
  "Read the unscaled decimal value from STREAM."
  (declare (ignore writer-schema)
           (inline read-twos-complement)
           (optimize (speed 3) (safety 0)))
  (let* ((fixed-or-bytes-schema (decimal-schema-underlying-schema schema))
         (bytes (deserialize fixed-or-bytes-schema stream))
         (unscaled (read-twos-complement bytes)))
    (handler-case
        (assert-valid schema unscaled)
      (error ()
        (cerror "Return ~S anyway."
                "~S is too large for precision ~S"
                unscaled
                (decimal-schema-precision schema))))
    unscaled))

(defun! get-min-buf-length (integer schema)
    ((integer (or (eql bytes-schema) fixed-schema)) (integer 0))
  (if (fixed-schema-p schema)
      (fixed-schema-size schema)
      (ceiling (1+ (integer-length integer)) 8)))

(defmethod serialize ((schema decimal-schema)
                      (unscaled integer)
                      &optional stream)
  "Write UNSCALED to STREAM."
  (declare (inline get-min-buf-length write-twos-complement)
           (optimize (speed 3) (safety 0)))
  (let* ((fixed-or-bytes-schema (decimal-schema-underlying-schema schema))
         (min-length (get-min-buf-length unscaled fixed-or-bytes-schema))
         (bytes (make-array min-length)))
    (write-twos-complement unscaled bytes)
    (serialize fixed-or-bytes-schema bytes stream)))
