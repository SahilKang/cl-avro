;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defgeneric stream-read-item (stream)
  (:documentation
   "Read next item from STREAM or :EOF."))


(defclass block-input-stream (trivial-gray-streams:fundamental-binary-input-stream)
  ((stream
    :initform (error "Must supply binary input stream")
    :initarg :stream
    :type stream
    :documentation "Binary input stream to read from.")
   (count
    :initform (error "Must supply count")
    :initarg :count
    :reader block-count
    :type long-schema
    :documentation "Number of items in this block.")
   (position
    :initform 0
    :type long-schema
    :documentation "Number of items read from this block.")
   (size
    :initform nil
    :initarg :size
    :reader block-size
    :type (or null long-schema)
    :documentation "Number of bytes in this block.")
   (schema
    :initform (error "Must supply schema")
    :initarg :schema
    :type avro-schema
    :documentation "Schema for the items contained in this block."))
  (:documentation
   "Represents an avro block which composes array and map types."))

;; TODO change this to a function
(defgeneric end-of-block-p (stream)
  (:method ((stream block-input-stream))
    (declare (optimize (speed 3) (safety 0)))
    (with-slots (position count) stream
      (declare (long-schema position count))
      (>= position count))))

(defmethod stream-element-type ((stream block-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte ((stream block-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots ((input-stream stream)) stream
    (if (end-of-block-p stream)
        :eof
        (read-byte input-stream nil :eof))))

(defmethod stream-read-item ((stream block-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots ((input-stream stream) schema position) stream
    (if (end-of-block-p stream)
        :eof
        (prog1 (deserialize schema input-stream)
          (incf (the long-schema position))))))


(defclass blocked-input-stream (trivial-gray-streams:fundamental-binary-input-stream)
  ((stream
    :initform (error "Must supply binary input stream")
    :initarg :stream
    :type stream)
   (block-stream
    :type block-input-stream
    :documentation "Stream used to read constituent blocks.")
   (schema
    :initform (error "Must supply schema")
    :initarg :schema
    :reader schema
    :type avro-schema
    :documentation "The schema object used to deserialize constituent items."))
  (:documentation
   "Avro arrays and maps are pretty much the same so this is common functionality."))

(defun! get-next-block (stream schema)
    ((stream avro-schema) block-input-stream)
  (let* ((count (deserialize 'long-schema stream))
         (size (when (< count 0)
                 (deserialize 'long-schema stream))))
    (declare (long-schema count size))
    (make-instance 'block-input-stream
                   :stream stream
                   :count (abs count)
                   :size size
                   :schema schema)))

(defmethod initialize-instance :after ((stream blocked-input-stream) &key)
  (declare (inline get-next-block)
           (optimize (speed 3) (safety 0)))
  (with-slots (schema block-stream (input-stream stream)) stream
    (setf block-stream (get-next-block input-stream schema))))

;; TODO change this to a function
(defgeneric last-block-p (stream)
  (:method ((stream blocked-input-stream))
    (declare (optimize (speed 3) (safety 0)))
    (with-slots (block-stream) stream
      (zerop (the long-schema (block-count block-stream))))))

(defmethod stream-read-item :around ((stream blocked-input-stream))
  (declare (inline get-next-block)
           (optimize (speed 3) (safety 0)))
  (with-slots (block-stream schema (input-stream stream)) stream
    (tagbody top
       (cond
         ((last-block-p stream)
          (return-from stream-read-item :eof))
         ((end-of-block-p block-stream)
          (setf block-stream (get-next-block input-stream schema))
          (go top))))
    (call-next-method)))


(defclass array-input-stream (blocked-input-stream)
  ()
  (:documentation
   "Represents an avro array during deserialization."))

(defmethod stream-read-item ((stream array-input-stream))
  "Returns the next array item from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (block-stream) stream
    (stream-read-item block-stream)))


(defclass map-input-stream (blocked-input-stream)
  ()
  (:documentation
   "Represents an avro map during deserialization."))

(defmethod stream-read-item ((stream map-input-stream))
  "Returns the next (key . value) pair from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (block-stream) stream
    (let ((key (deserialize 'string-schema block-stream))
          (value (stream-read-item block-stream)))
      (cons key value))))
