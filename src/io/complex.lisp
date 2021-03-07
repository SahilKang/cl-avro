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
(defpackage #:cl-avro.io.complex
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:stream #:cl-avro.io.block-stream))
  (:import-from #:cl-avro.io.base
                #:serialize
                #:deserialize)
  (:export #:serialize
           #:deserialize))
(in-package #:cl-avro.io.complex)

;;; fixed schema

(defmethod serialize ((object schema:fixed-object) &key stream)
  "Write fixed bytes into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (write-sequence (cl-avro.schema.complex.fixed::buffer object) stream)
  (values))

(defmethod deserialize ((schema schema:fixed) (stream stream) &key)
  "Read a fixed number of bytes from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((size (schema:size schema))
         (bytes (make-instance schema)))
    (unless (= (read-sequence (cl-avro.schema.complex.fixed::buffer bytes) stream) size)
      (error 'end-of-file :stream *error-output*))
    bytes))

;;; union schema

(defmethod serialize ((object schema:union-object) &key stream)
  "Write tagged union into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((position (nth-value 1 (schema:which-one object))))
    (serialize position :stream stream))
  (serialize (schema:object object) :stream stream)
  (values))

(defmethod deserialize ((schema schema:union) (stream stream) &key)
  "Read a tagged union from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((schemas (schema:schemas schema))
         (position (deserialize 'schema:int stream))
         (chosen-schema (elt schemas position))
         (value (deserialize chosen-schema stream)))
    (declare ((simple-array schema:schema (*)) schemas))
    (make-instance schema :object value)))

;;; array schema

(defmethod serialize ((object schema:array-object) &key stream)
  "Write array into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((count (length object)))
    (unless (zerop count)
      (serialize count :stream stream)
      (flet ((serialize (elt)
               (serialize elt :stream stream)))
        (map nil #'serialize object))))
  (serialize 0 :stream stream)
  (values))

(defmethod deserialize ((schema schema:array) (stream stream) &key)
  "Read an array from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with array-stream = (make-instance 'stream:array-input-stream
                                       :schema (schema:items schema)
                                       :stream stream)
    and vector = (make-instance schema)

    for item = (stream:read-item array-stream)
    until (eq item :eof)
    do (schema:push item vector)

    finally
       (return vector)))

;;; map schema

(defmethod serialize ((object schema:map-object) &key stream)
  "Write map into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((hash-table (schema:map object))
         (count (hash-table-count hash-table)))
    (unless (zerop count)
      (serialize count :stream stream)
      (flet ((serialize (key value)
               (serialize key :stream stream)
               (serialize value :stream stream)))
        (maphash #'serialize hash-table))))
  (serialize 0 :stream stream)
  (values))

(defmethod deserialize ((schema schema:map) (stream stream) &key)
  "Read a map from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with map-stream = (make-instance 'stream:map-input-stream
                                     :schema (schema:values schema)
                                     :stream stream)
    and hash-table = (make-hash-table :test #'equal)

    for pair = (stream:read-item map-stream)
    until (eq pair :eof)
    for (key . value) = pair
    do (setf (gethash key hash-table) value)

    finally
       (return (make-instance schema :map hash-table))))

;;; enum schema

(defmethod serialize ((object schema:enum-object) &key stream)
  "Write enum into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((position (nth-value 1 (schema:which-one object))))
    (serialize position :stream stream))
  (values))

(defmethod deserialize ((schema schema:enum) (stream stream) &key)
  "Read an enum from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  ;; TODO should just have a factory method on the class/schema
  ;; i.e intern enum objects on the class/schema
  (let* ((symbols (schema:symbols schema))
         (position (deserialize 'schema:int stream))
         (chosen-enum (elt symbols position)))
    (declare ((simple-array schema:name (*)) symbols))
    (make-instance schema :enum chosen-enum)))

;;; record schema

(defmethod serialize ((object schema:record-object) &key stream)
  "Write record into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with fields of-type (simple-array schema:field (*)) = (schema:fields
                                                           (class-of object))

    for field of-type schema:field across fields
    for value = (schema:field object (schema:name field))

    do (serialize value :stream stream)

    finally
       (return (values))))

(defmethod deserialize ((schema schema:record) (stream stream) &key)
  "Read a record from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with fields of-type (simple-array schema:field (*)) = (schema:fields schema)
    and initargs of-type list = nil

    for field of-type schema:field across fields
    for value = (deserialize (schema:type field) stream)

    do
       (push value initargs)
       (push (intern (schema:name field) 'keyword) initargs)

    finally
       (return (apply #'make-instance schema initargs))))
