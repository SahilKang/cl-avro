;;; Copyright 2021, 2024 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.double
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:ieee-754 #:cl-avro.internal.ieee-754))
  (:import-from #:cl-avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:cl-avro.internal.type
                #:uint8)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+))
(in-package #:cl-avro.internal.double)

(macrolet
    ((defdouble ()
       (assert (= 53 (float-digits 1d0)))
       `(defprimitive api:double double-float
          "Avro double schema.")))
  (defdouble))

(defmethod api:schema-of
    ((object double-float))
  (declare (ignore object))
  'api:double)

(ieee-754:implement 64)

(defmethod internal:fixed-size
    ((schema (eql 'api:double)))
  (declare (ignore schema))
  8)

(defmethod api:serialized-size
    ((object double-float))
  (declare (ignore object))
  8)

(defmethod internal:serialize
    ((object double-float) (into vector) &key (start 0))
  (serialize-into-vector object into start))

(defmethod internal:serialize
    ((object double-float) (into stream) &key)
  (serialize-into-stream object into))

(defmethod api:serialize
    ((object double-float)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 18 8) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:double)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:double)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:double)) (input vector) &optional start)
  (declare (ignore schema input start))
  8)

(defmethod internal:skip
    ((schema (eql 'api:double)) (input stream) &optional start)
  (declare (ignore schema start))
  (loop repeat 8 do (read-byte input))
  8)

(defmethod api:compare
    ((schema (eql 'api:double)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:double)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object double-float) (schema (eql 'api:double)))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object single-float) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce object 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql 'api:double)))
  (declare (ignore schema)
           (api:long object))
  (coerce object 'api:double))

(defmethod internal:serialize-field-default
    ((default double-float))
  default)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:double)) (default double-float))
  (declare (ignore schema))
  default)
