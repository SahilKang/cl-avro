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
(defpackage #:cl-avro.object-container-file.file-output-stream
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:header #:cl-avro.object-container-file.header)
   (#:block #:cl-avro.object-container-file.file-block))
  (:export #:file-output-stream
           #:write-block))
(in-package #:cl-avro.object-container-file.file-output-stream)

(defclass file-output-stream ()
  ((stream
    :reader wrapped-stream
    :type stream)
   (header
    :reader header:header
    :type header:header)
   (wrote-header-p
    :initform nil
    :accessor wrote-header-p
    :type boolean))
  (:documentation
   "An output stream for an avro object container file."))

(defmethod initialize-instance :after
    ((instance file-output-stream)
     &key
       (output (error "Must supply OUTPUT"))
       (meta (error "Must supply META"))
       (sync (make-instance 'header:sync)))
  (with-slots (stream header) instance
    (setf stream (if (streamp output)
                     output
                     (make-instance 'io:memory-output-stream :bytes output))
          header (make-instance 'header:header :sync sync :meta meta))))

(declaim
 (ftype (function (file-output-stream (simple-array schema:schema (*)))
                  (values &optional))
        write-block))
;; TODO maybe return file-block
(defun write-block (file-output-stream objects)
  (with-accessors
        ((header header:header)
         (stream wrapped-stream)
         (wrote-header-p wrote-header-p))
      file-output-stream
    (unless wrote-header-p
      (io:serialize header :stream stream)
      (setf wrote-header-p t))
    (io:serialize
     (block:make-file-block-objects :header header :objects objects)
     :stream stream))
  (values))
