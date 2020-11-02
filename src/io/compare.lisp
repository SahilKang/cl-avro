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

(defgeneric compare (schema left right)
  (:method (schema (left simple-vector) right)
    (declare (optimize (speed 3) (safety 0)))
    (let ((left (make-instance 'byte-vector-input-stream :bytes left)))
      (the (integer -1 1)
           (compare schema left right))))

  (:method (schema left (right simple-vector))
    (declare (optimize (speed 3) (safety 0)))
    (let ((right (make-instance 'byte-vector-input-stream :bytes right)))
      (the (integer -1 1)
           (compare schema left right))))

  (:documentation
   "Return 0, -1, or 1 if LEFT is equal to, less than, or greater than RIGHT.

LEFT and RIGHT should be avro serialized data.

LEFT and RIGHT may not necessarily be fully consumed."))

(defmacro defcompare ((schema-type) &body body)
  (declare (symbol schema-type))
  (let ((schema-type (to-method-specifier schema-type))
        (declarations (rip-out-declarations body)))
    `(defmethod compare ((schema ,schema-type) (left stream) (right stream))
       (declare (optimize (speed 3) (safety 0))
                ,@declarations)
       (the (integer -1 1)
            ,@body))))

;; null-schema

(defcompare (null-schema)
  0)

;; boolean-schema

(defcompare (boolean-schema)
  (let ((left (read-byte left))
        (right (read-byte right)))
    (declare ((integer 0 1) left right))
    (- left right)))

;; int-schema

(defmacro compare-number (left right)
  (declare (symbol left right))
  `(cond
     ((= ,left ,right) 0)
     ((< ,left ,right) -1)
     (t 1)))

(defmacro defcompare-number (schema-type)
  (declare (primitive-schema schema-type))
  `(defcompare (,schema-type)
     (let ((left (deserialize ',schema-type left))
           (right (deserialize ',schema-type right)))
       (declare (,schema-type left right))
       (compare-number left right))))

(defcompare-number int-schema)

;; long-schema

(defcompare-number long-schema)

;; float-schema

(defcompare-number float-schema)

;; double-schema

(defcompare-number double-schema)

;; bytes-schema

(defun! compare-bytes (left right)
    ((vector[byte] vector[byte]) (integer -1 1))
  (loop
    with left-length of-type long-schema = (length left)
    and right-length = (length right)

    for x of-type (unsigned-byte 8) across left
    for y of-type (unsigned-byte 8) across right

    if (< x y)
      return -1
    else
      if (> x y)
        return 1

    finally (return (compare-number left-length right-length))))

(defcompare (bytes-schema)
  (declare (inline compare-bytes))
  (let ((left (deserialize 'bytes-schema left))
        (right (deserialize 'bytes-schema right)))
    (declare (vector[byte] left right))
    (compare-bytes left right)))

;; string-schema

(defcompare (string-schema)
  (compare 'bytes-schema left right))

;; fixed-schema

(defcompare (fixed-schema)
  (declare (inline compare-bytes))
  (let ((left (deserialize schema left))
        (right (deserialize schema right)))
    (declare (vector[byte] left right))
    (compare-bytes left right)))

;; array-schema

(defclass binary-blocked-input-stream (blocked-input-stream)
  ((schema
    :initform 'null-schema))
  (:documentation
   "A blocked-input-stream that does not perform deserialization."))

(defmethod stream-read-item ((stream binary-blocked-input-stream))
  "Returns the internal block-input-stream ready to consume the next item."
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (block-stream) stream
    block-stream))

(defun! incf-position (stream &optional (delta 1))
    ((binary-blocked-input-stream &optional fixnum) long-schema)
  (with-slots (block-stream) stream
    (with-slots (position) block-stream
      (incf (the long-schema position) delta))))

(defcompare (array-schema)
  (loop
    with items = (array-schema-items schema)
    and left = (make-instance 'binary-blocked-input-stream :stream left)
    and right = (make-instance 'binary-blocked-input-stream :stream right)

    for x = (stream-read-item left)
    for y = (stream-read-item right)
    until (or (eq x :eof) (eq y :eof))

    for compare of-type (integer -1 1) = (prog1 (compare items x y)
                                           (incf-position left)
                                           (incf-position right))
    unless (zerop compare)
      return compare

    finally
       (return
         (cond
           ((and (eq x :eof) (eq y :eof)) 0)
           ((eq x :eof) -1)
           (t 1)))))

;; enum-schema

(defcompare (enum-schema)
  (compare 'int-schema left right))

;; union-schema

(defcompare (union-schema)
  (let* ((left-position (deserialize 'long-schema left))
         (right-position (deserialize 'long-schema right))
         (compare (compare-number left-position right-position)))
    (declare (long-schema left-position right-position)
             ((integer -1 1) compare))
    (if (not (zerop compare))
        compare
        (let* ((schemas (union-schema-schemas schema))
               (chosen-schema (svref schemas left-position)))
          (compare chosen-schema left right)))))

;; record-schema

(defgeneric skip-field (schema stream))

(defmacro defskip ((field-type &optional (stream-type 'stream)) &body body)
  (declare (symbol field-type stream-type))
  (let ((field-type (to-method-specifier field-type))
        (declarations (rip-out-declarations body)))
    `(defmethod skip-field ((schema ,field-type) (stream ,stream-type))
       (declare (optimize (speed 3) (safety 0))
                ,@declarations)
       ,@body)))

(defskip (null-schema))

(defskip (boolean-schema)
  (read-byte stream))

(defskip (boolean-schema byte-vector-input-stream)
  (with-slots (position) stream
    (incf (the long-schema position))))

(defskip (int-schema)
  (deserialize 'int-schema stream))

(defskip (long-schema)
  (deserialize 'long-schema stream))

;; TODO look into the long-schema declarations
(defgeneric skip-bytes (stream count)
  (:method ((stream stream) (count integer))
    (declare (long-schema count)
             (optimize (speed 3) (safety 0)))
    (loop repeat count do (read-byte stream)))

  (:method ((stream byte-vector-input-stream) (count integer))
    (declare (long-schema count)
             (optimize (speed 3) (safety 0)))
    (with-slots (position) stream
      (incf (the long-schema position) count))))

(defskip (float-schema)
  (skip-bytes stream 4))

(defskip (double-schema)
  (skip-bytes stream 8))

(defskip (bytes-schema)
  (let ((count (deserialize 'long-schema stream)))
    (skip-bytes stream count)))

(defskip (string-schema)
  (skip-field 'bytes-schema stream))

(defskip (fixed-schema)
  (let ((size (fixed-schema-size schema)))
    (skip-bytes stream size)))

(defskip (union-schema)
  (let* ((schemas (union-schema-schemas schema))
         (position (deserialize 'long-schema stream))
         (chosen-schema (svref schemas position)))
    (skip-field chosen-schema stream)))

(defskip (array-schema)
  (loop
    with items = (array-schema-items schema)
    and blocked-stream = (make-instance 'binary-blocked-input-stream :stream stream)

    for block-stream = (stream-read-item blocked-stream)
    until (eq block-stream :eof)

    for size = (block-size block-stream)
    for count of-type long-schema = (block-count block-stream)

    if size do
      (skip-bytes stream size)
    else do
      (loop repeat count do (skip-field items stream))

    do (incf-position blocked-stream count)))

(defskip (map-schema)
  (loop
    with values = (map-schema-values schema)
    and blocked-stream = (make-instance 'binary-blocked-input-stream :stream stream)

    for block-stream = (stream-read-item blocked-stream)
    until (eq block-stream :eof)

    for size = (block-size block-stream)
    for count of-type long-schema = (block-count block-stream)

    if size do
      (skip-bytes stream size)
    else do
      (loop
        repeat count
        do
           (skip-field 'string-schema stream)
           (skip-field values stream))

    do (incf-position blocked-stream count)))

(defskip (enum-schema)
  (deserialize 'int-schema stream))

(defskip (record-schema)
  (loop
    with fields = (record-schema-fields schema)

    for field across fields
    for type = (field-schema-type field)

    do (skip-field type stream)))


(defcompare (record-schema)
  (loop
    with fields = (record-schema-fields schema)

    for field across fields
    for order = (field-schema-order field)
    for type = (field-schema-type field)

    if (string= order "ignore") do
      (skip-field type left)
      (skip-field type right)

    else do
      (let ((compare (compare type left right)))
        (declare ((integer -1 1) compare))
        (unless (zerop compare)
          (return-from compare
            (if (string= order "ascending")
                compare
                (- compare)))))

    finally (return 0)))
