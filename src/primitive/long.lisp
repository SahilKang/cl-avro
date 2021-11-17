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
(defpackage #:cl-avro.internal.long
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
(in-package #:cl-avro.internal.long)

(defprimitive api:long (signed-byte 64)
  "Avro long schema.")

(defmethod api:schema-of
    ((object integer))
  (etypecase object
    (api:int 'api:int)
    (api:long 'api:long)))

(zigzag:implement 64)

(defmethod internal:fixed-size
    ((schema (eql 'api:long)))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object integer))
  (serialized-size object))

(defmethod api:serialize
    ((schema (eql 'api:long)) &key)
  (declare (ignore schema))
  +json+)

(defmethod internal:serialize
    ((object integer) (into vector) &key (start 0))
  (serialize-into-vector object into start))

(defmethod internal:serialize
    ((object integer) (into stream) &key)
  (serialize-into-stream object into))

(defmethod api:serialize
    ((object integer)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (serialized-size object)) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:long)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:long)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:long)) (input vector) &optional (start 0))
  (declare (ignore schema))
  (nth-value 1 (deserialize-from-vector input start)))

(defmethod internal:skip
    ((schema (eql 'api:long)) (input stream) &optional start)
  (declare (ignore schema start))
  (nth-value 1 (deserialize-from-stream input)))

(defmethod api:compare
    ((schema (eql 'api:long)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:long)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object integer) (schema (eql 'api:long)))
  (declare (ignore schema)
           (api:long object))
  object)

(defmethod internal:serialize-field-default
    ((default integer))
  (declare ((or api:int api:long) default))
  default)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:long)) (default integer))
  (declare (ignore schema)
           (api:long default))
  default)
