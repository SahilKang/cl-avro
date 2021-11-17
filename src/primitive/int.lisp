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
(defpackage #:cl-avro.internal.int
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:zigzag #:cl-avro.internal.zigzag))
  (:import-from #:cl-avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:cl-avro.internal.type
                #:uint8)
  (:export #:+jso+
           #:+json+
           #:size
           #:+crc-64-avro+
           #:serialized-size
           #:serialize-into-vector
           #:serialize-into-stream
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.internal.int)

(defprimitive api:int (signed-byte 32)
  "Avro int schema.")

(zigzag:implement 32)

(defmethod internal:fixed-size
    ((schema (eql 'api:int)))
  (declare (ignore schema))
  nil)

(defmethod api:serialize
    ((schema (eql 'api:int)) &key)
  (declare (ignore schema))
  +json+)

(defmethod api:deserialize
    ((schema (eql 'api:int)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:int)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:int)) (input vector) &optional (start 0))
  (declare (ignore schema))
  (nth-value 1 (deserialize-from-vector input start)))

(defmethod internal:skip
    ((schema (eql 'api:int)) (input stream) &optional start)
  (declare (ignore schema start))
  (nth-value 1 (deserialize-from-stream input)))

(defmethod api:compare
    ((schema (eql 'api:int)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:int)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object integer) (schema (eql 'api:int)))
  (declare (ignore schema)
           (api:int object))
  object)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:int)) (default integer))
  (declare (ignore schema)
           (api:int default))
  default)
