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
                #:deserialize
                #:serialized-size)
  (:export #:serialize
           #:deserialize
           #:serialized-size))
(in-package #:cl-avro.io.complex)

;;; fixed schema

(defmethod serialized-size ((object schema:fixed-object))
  (length object))

(defmethod serialize ((object schema:fixed-object) &key stream)
  "Write fixed bytes into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (write-sequence (schema:raw-buffer object) stream)
  (values))

(defmethod deserialize ((schema schema:fixed) (stream stream) &key)
  "Read a fixed number of bytes from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((size (schema:size schema))
         (bytes (make-instance schema)))
    (unless (= (read-sequence (schema:raw-buffer bytes) stream) size)
      (error 'end-of-file :stream *error-output*))
    bytes))

;;; union schema

(defmethod serialized-size ((object schema:union-object))
  (let ((position (nth-value 1 (schema:which-one object))))
    (+ (serialized-size position)
       (serialized-size (schema:object object)))))

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

(defmethod serialized-size ((object schema:array-object))
  (let ((count (length object)))
    (if (zerop count)
        1
        (1+ (reduce (lambda (agg x)
                      (+ agg (serialized-size x)))
                    object
                    :initial-value (serialized-size count))))))

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

(defmethod serialized-size ((object schema:map-object))
  (let ((count (schema:generic-hash-table-count object))
        (result 1))
    (unless (zerop count)
      (incf result (serialized-size count))
      (flet ((incf-result (key value)
               (incf result (serialized-size key))
               (incf result (serialized-size value))))
        (schema:hashmap #'incf-result object)))
    result))

(defmethod serialize ((object schema:map-object) &key stream)
  "Write map into STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let ((count (schema:generic-hash-table-count object)))
    (declare (fixnum count))
    (unless (zerop count)
      (serialize count :stream stream)
      (flet ((serialize (key value)
               (serialize key :stream stream)
               (serialize value :stream stream)))
        (schema:hashmap #'serialize object))))
  (serialize 0 :stream stream)
  (values))

(defmethod deserialize ((schema schema:map) (stream stream) &key)
  "Read a map from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with map-stream = (make-instance 'stream:map-input-stream
                                     :schema (schema:values schema)
                                     :stream stream)
    and hash-table = (make-instance schema)

    for pair = (stream:read-item map-stream)
    until (eq pair :eof)
    for (key . value) = pair
    do (setf (schema:hashref key hash-table) value)

    finally
       (return hash-table)))

;;; enum schema

(defmethod serialized-size ((object schema:enum-object))
  (let ((position (nth-value 1 (schema:which-one object))))
    (serialized-size position)))

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

(defmethod serialized-size ((object schema:record-object))
  (reduce (lambda (agg field)
            (+ agg (serialized-size
                    (schema:field object (schema:name field)))))
          (schema:fields (class-of object))
          :initial-value 0))

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
