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
(defpackage #:cl-avro.object-container-file.file-input-stream
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:header #:cl-avro.object-container-file.header)
   (#:block #:cl-avro.object-container-file.file-block))
  (:export #:file-input-stream
           #:skip-block
           #:read-block))
(in-package #:cl-avro.object-container-file.file-input-stream)

;;; peekable binary stream

(defclass peekable-binary-input-stream
    (trivial-gray-streams:fundamental-binary-input-stream)
  ((next
    :reader peek
    :type (or null (unsigned-byte 8)))
   (stream
    :initarg :stream
    :type stream))
  (:default-initargs
   :stream (error "Must supply STREAM")))

(defmethod initialize-instance :after
    ((instance peekable-binary-input-stream) &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (next stream) instance
    (setf next (read-byte stream nil nil))))

(defmethod stream-element-type
    ((instance peekable-binary-input-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte
    ((instance peekable-binary-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (next stream) instance
    (if (null next)
        :eof
        (prog1 next
          (setf next (read-byte stream nil nil))))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((instance peekable-binary-input-stream)
     (vector simple-array)
     (start integer)
     (end integer)
     &key)
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (with-slots (next stream) instance
    (if (null next)
        start
        (prog1 (read-sequence vector stream :start (1+ start) :end end)
          (setf (elt vector start) next
                next (read-byte stream nil nil))))))

;;; file input stream

(defclass file-input-stream ()
  ((stream
    :reader wrapped-stream
    :type peekable-binary-input-stream)
   (header
    :reader header:header
    :type header:header)
   (file-block
    :accessor file-block
    :type (or null block:file-block)))
  (:documentation
   "An input stream for an avro object container file."))

(defmethod initialize-instance :after
    ((instance file-input-stream) &key (input (error "Must supply INPUT")))
  (with-slots (stream header file-block) instance
    (setf stream (make-instance
                  'peekable-binary-input-stream
                  :stream (if (streamp input)
                              input
                              (make-instance 'io:memory-input-stream :bytes input)))
          header (io:deserialize (find-class 'header:header) stream)
          file-block (io:deserialize (find-class 'block:file-block) stream))))

(declaim
 (ftype (function (file-input-stream) (values &optional)) set-next-file-block))
(defun set-next-file-block (file-input-stream)
  (with-accessors
        ((stream wrapped-stream)
         (file-block file-block))
      file-input-stream
    (setf file-block
          (when (peek stream)
            (io:deserialize (find-class 'block:file-block) stream))))
  (values))

(declaim
 (ftype (function (file-input-stream)
                  (values (or null block:file-block) &optional))
        skip-block))
(defun skip-block (file-input-stream)
  (prog1 (file-block file-input-stream)
    (set-next-file-block file-input-stream)))

(declaim
 (ftype
  (function (file-input-stream)
            (values (or null (simple-array schema:schema (*))) &optional))
  read-block))
(defun read-block (file-input-stream)
  (let ((header (header:header file-input-stream))
        (file-block (skip-block file-input-stream)))
    (when file-block
      (block:file-block-objects-objects
       (io:deserialize header file-block)))))
