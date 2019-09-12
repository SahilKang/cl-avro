;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

;;; fixed-schema

(defmethod validp ((schema fixed-schema) object)
  (and (typep object '(typed-vector (unsigned-byte 8)))
       (= (length object) (size schema))))

(defmethod deserialize (stream (schema fixed-schema))
  "Read using the number of bytes declared in the schema."
  (let ((buf (make-array (size schema) :element-type '(unsigned-byte 8))))
    (loop
       for i below (size schema)
       do (setf (elt buf i) (read-byte stream nil :eof)))
    buf))

(defmethod serialize (stream (schema fixed-schema) object)
  (loop
     for i below (length object)
     do (write-byte (elt object i) stream)))

;;; union-schema

(defclass union-value ()
  ((value
    :initform (error "Must supply :value")
    :initarg :value
    :reader value)
   (position
    :initform (error "Must supply :position")
    :initarg :position
    :type (integer 0))))

(defmethod print-object ((union-value union-value) stream)
  (with-slots (value) union-value
    (format stream "~A" value)))

(defmethod validp ((schema union-schema) object)
  (some (lambda (s) (validp s object)) (schemas schema)))

(defmethod validp ((schema union-schema) (object union-value))
  (with-slots (value position) object
    (when (< position (length (schemas schema)))
      (validp (elt (schemas schema) position) value))))

(defmethod deserialize (stream (schema union-schema))
  "Read with a long indicating the union type and then the value itself."
  (let* ((pos (deserialize stream 'long-schema))
         (val (deserialize stream (elt (schemas schema) pos))))
    (make-instance 'union-value :value val :position pos)))

(defmethod serialize (stream (schema union-schema) (object union-value))
  (with-slots (value position) object
    (serialize stream 'long-schema position)
    (serialize stream (elt (schemas schema) position) value)))

(defmethod serialize (stream (schema union-schema) object)
  (let ((pos (loop
                for schema across (schemas schema) and i from 0
                when (validp schema object)
                collect i)))
    (cond
      ((zerop (length pos))
       (error "~&Object doesn't match any union schema: ~A" object))
      ((> (length pos) 1)
       (error "~&Object matches multiple union schemas: ~A" object))
      (t
       (let ((union-value (make-instance 'union-value
                                         :value object
                                         :position (first pos))))
         (serialize stream schema union-value))))))

;;; array-schema

(defmethod validp ((schema array-schema) object)
  (and (typep object 'sequence)
       (every (lambda (elt) (validp (item-schema schema) elt)) object)))

(defmethod deserialize (stream (schema array-schema))
  (loop
     with vector = (make-array 0 :adjustable t :fill-pointer 0)
     with array-stream = (make-instance 'array-input-stream
                                        :input-stream stream
                                        :schema (item-schema schema))

     for item = (stream-read-item array-stream)
     until (eq item :eof)
     do (vector-push-extend item vector)

     finally (return vector)))

(defmethod serialize (stream (schema array-schema) (object sequence))
  (let ((block-count (length object)))
    (unless (zerop block-count)
      (serialize stream 'long-schema block-count)
      (loop
         with schema = (item-schema schema)
         for i below block-count
         do (serialize stream schema (elt object i)))))
  (serialize stream 'long-schema 0))

;;; map-schema

(defmethod validp ((schema map-schema) object)
  (and (hash-table-p object)
       (eq (hash-table-test object) 'equal)
       (loop
          with value-schema = (value-schema schema)

          for key being the hash-keys of object using (hash-value value)

          always (and (validp 'string-schema key)
                      (validp value-schema value)))))

(defmethod deserialize (stream (schema map-schema))
  (loop
     with hash-table = (make-hash-table :test #'equal)
     with map-stream = (make-instance 'map-input-stream
                                      :input-stream stream
                                      :schema (value-schema schema))

     for pair = (stream-read-item map-stream)
     until (eq pair :eof)
     for (key val) = pair
     do (setf (gethash key hash-table) val)

     finally (return hash-table)))

(defmethod serialize (stream (schema map-schema) (object hash-table))
  (let ((block-count (hash-table-count object))
        (value-schema (value-schema schema)))
    (unless (zerop block-count)
      (serialize stream 'long-schema block-count)
      (maphash (lambda (k v)
                 (serialize stream 'string-schema k)
                 (serialize stream value-schema v))
               object)))
  (serialize stream 'long-schema 0))

;;; enum-schema

(defmethod validp ((schema enum-schema) object)
  (and (typep object 'avro-name)
       (not (null (position object (symbols schema) :test #'string=)))))

(defmethod deserialize (stream (schema enum-schema))
  (let ((pos (deserialize stream 'int-schema)))
    (elt (symbols schema) pos)))

(defmethod serialize (stream (schema enum-schema) (object string))
  (let ((pos (position object (symbols schema) :test #'string=)))
    (when (null pos)
      (error "~&String not in enum: ~A" object))
    (serialize stream 'int-schema pos)))

;;; record-schema

(defmethod validp ((schema record-schema) object)
  (and (typep object 'sequence)
       (= (length (field-schemas schema)) (length object))
       (every #'validp (field-schemas schema) object)))

(defmethod validp ((schema field-schema) object)
  (validp (field-type schema) object))

(defmethod deserialize (stream (schema record-schema))
  (let* ((field-schemas (field-schemas schema))
         (fields (make-array (length field-schemas))))
    (loop
       for schema across field-schemas and i from 0
       for value = (deserialize stream schema)
       do (setf (elt fields i) value))
    fields))

(defmethod deserialize (stream (schema field-schema))
  (deserialize stream (field-type schema)))

(defmethod serialize (stream (schema record-schema) object)
  (loop
     for schema across (field-schemas schema) and i from 0
     for field = (elt object i)
     do (serialize stream schema field)))

(defmethod serialize (stream (schema field-schema) object)
  (serialize stream (field-type schema) object))
