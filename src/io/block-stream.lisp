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

(in-package #:cl-user)
(defpackage #:cl-avro.io.block-stream
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:deserialize)
  (:export #:array-input-stream
           #:map-input-stream
           #:read-item
           #:binary-blocked-input-stream
           #:incf-position
           #:block-count
           #:block-size))
(in-package #:cl-avro.io.block-stream)

(defgeneric read-item (stream)
  (:documentation
   "Read next item from STREAM or :EOF."))

(deftype positive-long ()
  '(and (integer 0) schema:long))

;;; block-input-stream

(defclass block-input-stream
    (trivial-gray-streams:fundamental-binary-input-stream)
  ((stream
    :initarg :stream
    :reader input-stream
    :type stream
    :documentation "Binary input stream to read from.")
   (count
    :initarg :count
    :reader block-count
    :type positive-long
    :documentation "Number of items in this block.")
   (position
    :initform 0
    :accessor block-position
    :type positive-long
    :documentation "Number of items read from this block.")
   (size
    :initarg :size
    :reader block-size
    :type (or null positive-long)
    :documentation "Number of bytes in this block.")
   (schema
    :initarg :schema
    :reader schema
    :type schema:schema
    :documentation "Schema for the items contained in this block."))
  (:default-initargs
   :stream (error "Must supply STREAM")
   :count (error "Must supply COUNT")
   :size nil
   :schema (error "Must supply SCHEMA"))
  (:documentation
   "An avro block which composes array and map types."))

(defmethod stream-element-type ((stream block-input-stream))
  '(unsigned-byte 8))

(declaim
 (ftype (function (block-input-stream) (values boolean &optional))
        end-of-block-p)
 (inline end-of-block-p))
(defun end-of-block-p (block-stream)
  (declare (optimize (speed 3) (safety 0)))
  (let ((position (block-position block-stream))
        (count (block-count block-stream)))
    (declare (positive-long position count))
    (>= position count)))
(declaim (notinline end-of-block-p))

(defmethod trivial-gray-streams:stream-read-byte
    ((stream block-input-stream))
  (declare (optimize (speed 3) (safety 0))
           (inline end-of-block-p))
  (if (end-of-block-p stream)
      :eof
      (read-byte (input-stream stream))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream block-input-stream)
     (vector simple-array)
     (start integer)
     (end integer)
     &key)
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (read-sequence vector (input-stream stream) :start start :end end))

(defmethod read-item ((stream block-input-stream))
  (declare (optimize (speed 3) (safety 0))
           (inline end-of-block-p))
  (if (end-of-block-p stream)
      :eof
      (prog1 (deserialize (schema stream) (input-stream stream))
        (incf (the positive-long (block-position stream))))))

;;; blocked-input-stream-mixin

(defclass blocked-input-stream-mixin
    (trivial-gray-streams:fundamental-binary-input-stream)
  ((stream
    :initarg :stream
    :reader input-stream
    :type stream)
   (block-stream
    :type block-input-stream
    :accessor block-stream
    :documentation "Stream used to read constituent blocks.")
   (schema
    :initarg :schema
    :reader schema
    :type schema:schema
    :documentation "The schema used to deserialize constituent items."))
  (:default-initargs
   :stream (error "Must supply STREAM")
   :schema (error "Must supply SCHEMA"))
  (:documentation
   "Avro arrays and maps are pretty much the same so this is common functionality."))

(defmethod stream-element-type ((stream blocked-input-stream-mixin))
  '(unsigned-byte 8))

(declaim
 (ftype (function (stream schema:schema) (values block-input-stream &optional))
        %next-block)
 (inline %next-block))
(defun %next-block (stream schema)
  (declare (optimize (speed 3) (safety 0)))
  (let* ((count (deserialize 'schema:long stream))
         (size (when (minusp count)
                 (deserialize 'schema:long stream))))
    (declare (schema:long count)
             (positive-long size))
    (make-instance 'block-input-stream
                   :stream stream
                   :count (abs count)
                   :size size
                   :schema schema)))
(declaim (notinline %next-block))

(declaim
 (ftype (function (blocked-input-stream-mixin)
                  (values block-input-stream &optional))
        next-block)
 (inline next-block))
(defun next-block (stream)
  (declare (optimize (speed 3) (safety 0))
           (inline %next-block))
  (%next-block (input-stream stream) (schema stream)))
(declaim (notinline next-block))

(defmethod initialize-instance :after
    ((instance blocked-input-stream-mixin) &key)
  (declare (optimize (speed 3) (safety 0))
           (inline next-block))
  (setf (block-stream instance) (next-block instance)))

(declaim
 (ftype (function (blocked-input-stream-mixin) (values boolean &optional))
        last-block-p)
 (inline last-block-p))
(defun last-block-p (stream)
  (declare (optimize (speed 3) (safety 0)))
  (let ((count (block-count (block-stream stream))))
    (declare (positive-long count))
    (zerop count)))
(declaim (notinline last-block-p))

(defmethod read-item :around ((stream blocked-input-stream-mixin))
  (declare (optimize (speed 3) (safety 0))
           (inline next-block end-of-block-p))
  (with-accessors
        ((block-stream block-stream)
         (schema schema)
         (input-stream input-stream))
      stream
    (tagbody top
       (cond
         ((last-block-p stream)
          (return-from read-item :eof))
         ((end-of-block-p block-stream)
          (setf block-stream (next-block stream))
          (go top))))
    (call-next-method)))

;;; array-input-stream

(defclass array-input-stream (blocked-input-stream-mixin)
  ()
  (:documentation
   "An avro array during deserialization."))

(defmethod read-item ((stream array-input-stream))
  "Returns the next array item from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (read-item (block-stream stream)))

;;; map-input-stream

(defclass map-input-stream (blocked-input-stream-mixin)
  ()
  (:documentation
   "An avro map during deserialization."))

(defmethod read-item ((stream map-input-stream))
  "Returns the next (key . value) pair from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((block-stream (block-stream stream))
         (key (deserialize 'schema:string block-stream))
         (value (read-item block-stream)))
    (cons key value)))

;;; binary-blocked-input-stream

(defclass binary-blocked-input-stream (blocked-input-stream-mixin)
  ()
  (:default-initargs
   :schema 'schema:null)
  (:documentation
   "A blocked input stream that does not perform deserialization."))

(defmethod read-item ((stream binary-blocked-input-stream))
  "Returns the internal block-input-stream ready to consume the next item."
  (declare (optimize (speed 3) (safety 0)))
  (block-stream stream))

(declaim
 (ftype (function (binary-blocked-input-stream &optional positive-long)
                  (values positive-long &optional))
        incf-position)
 (inline incf-position))
(defun incf-position (stream &optional (delta 1))
  (declare (optimize (speed 3) (safety 0)))
  (incf (the positive-long
             (block-position (block-stream stream)))
        delta))
(declaim (notinline incf-position))
