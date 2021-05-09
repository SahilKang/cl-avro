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
(defpackage #:cl-avro.io.compare
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:deserialize)
  (:export #:compare))
(in-package #:cl-avro.io.compare)

(deftype comparison ()
  "The return type of compare."
  '(integer -1 1))

;; TODO be more permissive with bytes
(defgeneric compare (schema left right)
  (:method (schema (left simple-array) right)
    (check-type left (simple-array (unsigned-byte 8) (*)))
    (let ((left (flexi-streams:make-in-memory-input-stream left)))
      (the (values comparison &optional) (compare schema left right))))

  (:method (schema left (right simple-array))
    (check-type right (simple-array (unsigned-byte 8) (*)))
    (let ((right (flexi-streams:make-in-memory-input-stream right)))
      (the (values comparison &optional) (compare schema left right))))

  (:documentation
   "Return 0, -1, or 1 if LEFT is equal to, less than, or greater than RIGHT.

LEFT and RIGHT should be avro serialized data.

LEFT and RIGHT may not necessarily be fully consumed."))

;;; null schema

(defmethod compare
    ((schema (eql 'schema:null)) (left stream) (right stream))
  (declare (ignore schema left right))
  0)

;;; boolean schema

(defmethod compare
    ((schema (eql 'schema:boolean)) (left stream) (right stream))
  (declare (ignore schema))
  (let ((left (read-byte left))
        (right (read-byte right)))
    (declare ((integer 0 1) left right))
    (- left right)))

;;; numeric schemas

(defmacro compare-number (left right)
  (declare (symbol left right))
  `(cond
     ((= ,left ,right) 0)
     ((< ,left ,right) -1)
     (t 1)))

(macrolet
    ((defcompare (schema)
       (declare (schema:primitive-schema schema))
       `(defmethod compare
            ((schema (eql ',schema)) (left stream) (right stream))
          (declare (ignore schema))
          (let ((left (deserialize ',schema left))
                (right (deserialize ',schema right)))
            (declare (,schema left right))
            (compare-number left right)))))
  (defcompare schema:int)
  (defcompare schema:long)
  (defcompare schema:float)
  (defcompare schema:double))

;;; bytes schema

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*))
                   (simple-array (unsigned-byte 8) (*)))
                  (values comparison &optional))
        compare-bytes)
 (inline compare-bytes))
(defun compare-bytes (left right)
  (loop
    with left-length = (length left)
    and right-length = (length right)

    for lhs across left
    for rhs across right

    if (< lhs rhs)
      return -1
    else if (> lhs rhs)
      return 1

    finally
       (return (compare-number left-length right-length))))
(declaim (notinline compare-bytes))

(defmethod compare
    ((schema (eql 'schema:bytes)) (left stream) (right stream))
  (declare (ignore schema)
           (inline compare-bytes))
  (let ((left (deserialize 'schema:bytes left))
        (right (deserialize 'schema:bytes right)))
    (compare-bytes left right)))

;;; string schema

(defmethod compare
    ((schema (eql 'schema:string)) (left stream) (right stream))
  (declare (ignore schema))
  (compare 'schema:bytes left right))

;;; fixed schema

(defmethod compare
    ((schema schema:fixed) (left stream) (right stream))
  (declare (inline compare-bytes))
  (let ((left (schema:raw-buffer (deserialize schema left)))
        (right (schema:raw-buffer (deserialize schema right))))
    (compare-bytes left right)))

;;; array schema

(declaim
 (ftype (function (stream)
                  (values schema:long (or null schema:long) &optional))
        read-count-and-size)
 (inline read-count-and-size))
(defun read-count-and-size (stream)
  (let ((count (deserialize 'schema:long stream)))
    (if (minusp count)
        (values (abs count) (deserialize 'schema:long stream))
        (values count nil))))
(declaim (notinline read-count-and-size))

(defmethod compare
    ((schema schema:array) (left stream) (right stream))
  (declare (inline read-count-and-size))
  (loop
    with items = (schema:items schema)

    for left-count = (read-count-and-size left)
      then (if (zerop (decf left-count min-count))
               (read-count-and-size left)
               left-count)
    for right-count = (read-count-and-size right)
      then (if (zerop (decf right-count min-count))
               (read-count-and-size right)
               right-count)
    for min-count = (min left-count right-count)

    until (or (zerop left-count)
              (zerop right-count))

    do (loop
         repeat min-count
         for comparison of-type comparison = (compare items left right)
         unless (zerop comparison) do
           (return-from compare comparison))

    finally
       (return
         (cond
           ((and (zerop left-count) (zerop right-count)) 0)
           ((zerop left-count) -1)
           (t 1)))))

;;; enum schema

(defmethod compare
    ((schema schema:enum) (left stream) (right stream))
  (declare (ignore schema))
  (compare 'schema:int left right))

;;; union schema

(defmethod compare
    ((schema schema:union) (left stream) (right stream))
  (let* ((left-position (deserialize 'schema:int left))
         (right-position (deserialize 'schema:int right))
         (comparison (compare-number left-position right-position)))
    (declare (schema:int left-position right-position)
             (comparison comparison))
    (if (not (zerop comparison))
        comparison
        (let* ((schemas (schema:schemas schema))
               (chosen-schema (elt schemas left-position)))
          (declare ((simple-array schema:schema (*)) schemas))
          (compare chosen-schema left right)))))

;;; record schema

;; skip

(defgeneric skip (schema stream))

(defgeneric skip-bytes (stream count)
  (:method ((stream stream) (count integer))
    (declare (schema:long count))
    (loop repeat count do (read-byte stream))
    (values))

  (:method ((stream flexi-streams:in-memory-input-stream) (count integer))
    (declare (schema:long count))
    (file-position stream (+ (file-position stream) count))
    (values)))

(defmethod skip
    ((schema (eql 'schema:null)) stream)
  (declare (ignore schema stream))
  (values))

(defmethod skip
    ((schema (eql 'schema:boolean)) (stream stream))
  (declare (ignore schema))
  (skip-bytes stream 1))

(defmethod skip
    ((schema (eql 'schema:int)) (stream stream))
  (declare (ignore schema))
  (deserialize 'schema:int stream)
  (values))

(defmethod skip
    ((schema (eql 'schema:long)) (stream stream))
  (declare (ignore schema))
  (deserialize 'schema:long stream)
  (values))

(defmethod skip
    ((schema (eql 'schema:float)) (stream stream))
  (declare (ignore schema))
  (skip-bytes stream 4))

(defmethod skip
    ((schema (eql 'schema:double)) (stream stream))
  (declare (ignore schema))
  (skip-bytes stream 8))

(defmethod skip
    ((schema (eql 'schema:bytes)) (stream stream))
  (declare (ignore schema))
  (let ((count (deserialize 'schema:int stream)))
    (skip-bytes stream count)))

(defmethod skip
    ((schema (eql 'schema:string)) stream)
  (declare (ignore schema))
  (skip 'schema:bytes stream))

(defmethod skip
    ((schema schema:fixed) (stream stream))
  (let ((size (schema:size schema)))
    ;; even though size is (integer 0), schema:long should be fine
    (skip-bytes stream size)))

(defmethod skip
    ((schema schema:union) (stream stream))
  (let* ((schemas (schema:schemas schema))
         (position (deserialize 'schema:int stream))
         (chosen-schema (elt schemas position)))
    (declare ((simple-array schema:schema (*)) schemas))
    (skip chosen-schema stream))
  (values))

(defmethod skip
    ((schema schema:array) (stream stream))
  (loop
    with items = (schema:items schema)

    for count = (deserialize 'schema:long stream)
    until (zerop count)

    if (minusp count) do
      (skip-bytes stream (deserialize 'schema:long stream))
    else do
      (loop repeat count do (skip items stream))

    finally
       (return (values))))

(defmethod skip
    ((schema schema:map) (stream stream))
  (loop
    with values = (schema:values schema)

    for count = (deserialize 'schema:long stream)
    until (zerop count)

    if (minusp count) do
      (skip-bytes stream (deserialize 'schema:long stream))
    else do
      (loop
        repeat count
        do
           (skip 'schema:string stream)
           (skip values stream))

    finally
       (return (values))))

(defmethod skip
    ((schema schema:enum) (stream stream))
  (declare (ignore schema))
  (deserialize 'schema:int stream)
  (values))

(defmethod skip
    ((schema schema:record) (stream stream))
  (loop
    for field across (schema:fields schema)
    for type = (schema:type field)

    do (skip type stream))
  (values))

;; compare

(defmethod compare
    ((schema schema:record) (left stream) (right stream))
  (loop
    for field across (schema:fields schema)
    for order = (schema:order field)
    for type = (schema:type field)

    if (eq order 'schema:ignore) do
      (skip type left)
      (skip type right)
    else do
      (let ((comparison (compare type left right)))
        (declare (comparison comparison))
        (unless (zerop comparison)
          (return-from compare
            (if (eq order 'schema:ascending)
                comparison
                (- comparison)))))

    finally
       (return 0)))
