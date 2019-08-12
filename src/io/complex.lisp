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

(defmethod stream-deserialize (stream (schema fixed-schema))
  "Read using the number of bytes declared in the schema."
  (let ((buf (make-array (size schema) :element-type '(unsigned-byte 8))))
    (loop
       for i below (size schema)
       do (setf (elt buf i) (stream-read-byte stream)))
    buf))

(defmethod stream-serialize (stream (schema fixed-schema) object)
  (loop
     for i below (length object)
     do (stream-write-byte stream (elt object i))))

;;; union-schema

(defclass union-value ()
  ((value
    :initform (error "Must supply :value")
    :initarg :value
    :reader value)
   (position
    :initform (error "Must supply :position")
    :initarg :position
    :type '(integer 0))))

(defmethod print-object ((union-value union-value) stream)
  (with-slots (value) union-value
    (format stream "~A" value)))

(defmethod validp ((schema union-schema) object)
  (some (lambda (s) (validp s object)) (schemas schema)))

(defmethod validp ((schema union-schema) (object union-value))
  (with-slots (value position) object
    (when (< position (length (schemas schema)))
      (validp (elt (schemas schema) position) value))))

(defmethod stream-deserialize (stream (schema union-schema))
  "Read with a long indicating the union type and then the value itself."
  (let* ((pos (stream-deserialize stream 'long-schema))
         (val (stream-deserialize stream (elt (schemas schema) pos))))
    (make-instance 'union-value :value val :position pos)))

(defmethod stream-serialize (stream (schema union-schema) (object union-value))
  (with-slots (value position) object
    (stream-serialize stream 'long-schema position)
    (stream-serialize stream (elt (schemas schema) position) value)))

(defmethod stream-serialize (stream (schema union-schema) object)
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
         (stream-serialize stream schema union-value))))))

;;; array-schema

(defmethod validp ((schema array-schema) object)
  (and (typep object 'array-input-stream)
       (eq (item-schema schema) (schema object))))

(defmethod stream-deserialize (stream (schema array-schema))
  (make-instance 'array-input-stream
                 :input-stream stream
                 :schema (item-schema schema)))

(defun buffer-stream (stream-read-item buf-size handle-buf)
  (declare (function stream-read-item handle-buf)
           (integer buf-size))
  (let ((buf (make-array buf-size :fill-pointer 0 :adjustable nil)))
    (loop
       for item = (funcall stream-read-item)
       until (eq item :eof)

       when (= (length buf) (array-dimension buf 0))
       do
         (funcall handle-buf buf)
         (setf (fill-pointer buf) 0)

       do (vector-push item buf))
    (unless (zerop (length buf))
      (funcall handle-buf buf))))

(defmethod stream-serialize (stream (schema array-schema) (object array-input-stream))
  (flet ((stream-read-item ()
           (stream-read-item object))
         (handle-buf (buf)
           (stream-serialize stream 'long-schema (length buf))
           (loop
              with schema = (item-schema object)
              for elem across buf
              do (stream-serialize stream schema elem))))
    (buffer-stream #'stream-read-item (* 1024 1024) #'handle-buf)
    (stream-serialize stream 'long-schema 0)))

;;; map-schema

(defmethod validp ((schema map-schema) object)
  (and (typep object 'map-input-stream)
       (eq (value-schema schema) (schema object))))

(defmethod stream-deserialize (stream (schema map-schema))
  (make-instance 'map-input-stream
                 :input-stream stream
                 :schema (value-schema schema)))

(defmethod stream-serialize (stream (schema map-schema) (object map-input-stream))
  (flet ((stream-read-item ()
           (stream-read-item object))
         (handle-buf (buf)
           (stream-serialize stream 'long-schema (length buf))
           (loop
              with schema = (value-schema object)
              for (key val) across buf
              do
                (stream-serialize stream 'string-schema key)
                (stream-serialize stream schema val))))
    (buffer-stream #'stream-read-item (* 1024 1024) #'handle-buf)
    (stream-serialize stream 'long-schema 0)))

;;; enum-schema

(defmethod validp ((schema enum-schema) object)
  (and (typep object 'avro-name)
       (not (null (position object (symbols schema) :test #'string=)))))

(defmethod stream-deserialize (stream (schema enum-schema))
  (let ((pos (stream-deserialize stream 'int-schema)))
    (elt (symbols schema) pos)))

(defmethod stream-serialize (stream (schema enum-schema) (object string))
  (let ((pos (position object (symbols schema) :test #'string=)))
    (when (null pos)
      (error "~&String not in enum: ~A" object))
    (stream-serialize stream 'int-schema pos)))

;;; record-schema

(defmethod validp ((schema record-schema) object)
  (and (typep object 'sequence)
       (= (length (field-schemas schema)) (length object))
       (every #'validp (field-schemas schema) object)))

(defmethod validp ((schema field-schema) object)
  (validp (field-type schema) object))

(defmethod stream-deserialize (stream (schema record-schema))
  (let* ((field-schemas (field-schemas schema))
         (fields (make-array (length field-schemas))))
    (loop
       for schema across field-schemas and i from 0
       for value = (stream-deserialize stream schema)
       do (setf (elt fields i) value))
    fields))

(defmethod stream-deserialize (stream (schema field-schema))
  (stream-deserialize stream (field-type schema)))

(defmethod stream-serialize (stream (schema record-schema) object)
  (loop
     for schema across (field-schemas schema) and i from 0
     for field = (elt object i)
     do (stream-serialize stream schema field)))

(defmethod stream-serialize (stream (schema field-schema) object)
  (stream-serialize stream (field-type schema) object))
