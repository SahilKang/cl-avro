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

(defclass file-input-stream ()
  ((stream
    :reader wrapped-stream
    :type (or flexi-streams:flexi-input-stream
              flexi-streams:in-memory-input-stream))
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
    (setf stream (if (streamp input)
                     (flexi-streams:make-flexi-stream input :element-type '(unsigned-byte 8))
                     (flexi-streams:make-in-memory-input-stream input))
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
          (when (flexi-streams:peek-byte stream nil nil nil)
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
            (values (or null (simple-array schema:object (*))) &optional))
  read-block))
(defun read-block (file-input-stream)
  (let ((header (header:header file-input-stream))
        (file-block (skip-block file-input-stream)))
    (when file-block
      (block:file-block-objects-objects
       (block:from-file-block header file-block)))))
