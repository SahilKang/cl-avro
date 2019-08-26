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

(defgeneric validp (schema object)
  (:documentation
   "Determine if OBJECT is valid according to avro SCHEMA."))

(defgeneric stream-read-item (stream)
  (:documentation
   "Read next item from STREAM or :EOF."))

(defgeneric stream-deserialize (input-stream schema)
  (:documentation
   "Deserialize next object from INPUT-STREAM according to avro SCHEMA or :EOF."))

(defgeneric stream-serialize (output-stream schema object)
  (:documentation
   "Serialize OBJECT into OUTPUT-STREAM according to avro SCHEMA."))

(defmethod stream-serialize :before (output-stream schema object)
  (unless (validp schema object)
    (error "~&Object ~A does not match schema ~A" object schema)))


(defclass block-input-stream (fundamental-binary-input-stream)
  ((input-stream
    :initform (error "Must supply :input-stream")
    :initarg :input-stream)
   (item-count
    :initform (error "Must supply :item-count")
    :initarg :item-count
    :reader block-count
    :type 'long-schema
    :documentation "Number of items in this block.")
   (items-read
    :initform 0
    :type '(integer 0 (1- (expt 2 63)))
    :documentation "Number of items read from this stream.")
   (size
    :initform nil
    :initarg :size
    :reader block-size
    :type '(or nil (integer 0 (1- (expt 2 63))))
    :documentation "Number of bytes in this block.")
   (schema
    :initform (error "Must supply :schema")
    :initarg :schema
    :documentation "Schema describing the items contained in this block."))
  (:documentation
   "Represents an avro block which composes array and map types."))

(defmethod stream-read-item ((stream block-input-stream))
  (with-slots (items-read item-count schema input-stream) stream
    (if (= items-read item-count)
        :eof
        (let ((next-item (stream-deserialize input-stream schema)))
          (when (eq next-item :eof)
            (error 'end-of-file :stream *error-output*))
          (incf items-read)
          next-item))))


(defclass blocked-input-stream (fundamental-binary-input-stream)
  ((input-stream
    :initform (error "Must supply :input-stream")
    :initarg :input-stream)
   (schema
    :initform (error "Must supply :schema")
    :initarg :schema
    :reader schema
    :documentation "The schema object used to deserialize constituent items.")
   (block-stream
    :type 'block-input-stream
    :documentation "Stream used to read constituent blocks."))
  (:documentation
   "Avro arrays and maps are pretty much the same so this is a base class."))

(defgeneric last-block-p (stream))

(defgeneric get-next-block-item (stream)) ; specialized by derived classes

(defmethod initialize-instance :after ((stream blocked-input-stream) &key)
  (with-slots (schema block-stream) stream
    (setf block-stream (get-next-block stream schema))))

(defmethod last-block-p ((stream blocked-input-stream))
  (with-slots (block-stream) stream
    (zerop (block-count block-stream))))

(defun get-next-block (stream schema)
  (let* ((block-count (stream-deserialize stream 'long-schema))
         (block-size (when (< block-count 0)
                       (stream-deserialize stream 'long-schema))))
    (make-instance 'block-input-stream
                   :input-stream stream
                   :item-count (abs block-count)
                   :size block-size
                   :schema schema)))

(defmethod stream-read-item ((stream blocked-input-stream))
  (with-slots (block-stream schema) stream
    (let ((next-item (get-next-block-item stream)))
      (if (not (eq next-item :eof))
          next-item
          (if (last-block-p stream)
              :eof
              (progn
                (setf block-stream (get-next-block stream schema))
                (stream-read-item stream)))))))


(defclass array-input-stream (blocked-input-stream)
  ()
  (:documentation
   "Represents an avro array during deserialization."))

(defmethod get-next-block-item ((stream array-input-stream))
  (with-slots (block-stream) stream
    (stream-read-item block-stream)))


(defclass map-input-stream (blocked-input-stream)
  ()
  (:documentation
   "Represents an avro map during deserialization."))

(defmethod get-next-block-item ((stream map-input-stream))
  (with-slots (block-stream) stream
    (let ((key (stream-deserialize block-stream 'string-schema)))
      (if (eq key :eof)
          :eof
          (list key (stream-read-item block-stream))))))
