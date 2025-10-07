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
(defpackage #:cl-avro.internal.null
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+))
(in-package #:cl-avro.internal.null)

(defprimitive api:null null
  "Avro null schema.")

(defmethod api:schema-of
    ((object null))
  (declare (ignore object))
  'api:null)

(defmethod internal:fixed-size
    ((schema (eql 'api:null)))
  (declare (ignore schema))
  0)

(defmethod api:serialized-size
    ((object null))
  (declare (ignore object))
  0)

(defmethod internal:serialize
    ((object null) (into vector) &key (start 0))
  (declare (ignore object)
           (vector<uint8> into)
           (ufixnum start))
  (assert (<= start (length into)) (start)
          "Start ~S too large for vector of length ~S"
          start (length into))
  0)

(defmethod internal:serialize
    ((object null) (into stream) &key)
  (declare (ignore object into))
  0)

(defmethod api:serialize
    ((object null)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 10 0) :element-type 'uint8))
       (start 0))
  (declare (ignore object start))
  (values into (apply #'internal:serialize nil into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:null)) input &key)
  (declare (ignore schema input))
  (values nil 0))

(defmethod internal:skip
    ((schema (eql 'api:null)) input &optional start)
  (declare (ignore schema input start))
  0)

(defmethod api:compare
    ((schema (eql 'api:null)) left right &key)
  (declare (ignore schema left right))
  (values 0 0 0))

(defmethod api:coerce
    ((object null) (schema (eql 'api:null)))
  (declare (ignore object schema))
  nil)

(defmethod internal:serialize-field-default
    ((default null))
  (declare (ignore default))
  :null)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:null)) (default (eql :null)))
  (declare (ignore schema default))
  nil)
