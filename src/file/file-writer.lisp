;;; Copyright 2021, 2024 Google LLC
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
(defpackage #:cl-avro.internal.file.writer
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro))
  (:import-from #:cl-avro.internal.file.block
                #:objects->block
                #:codec->compress))
(in-package #:cl-avro.internal.file.writer)

;;; file-writer

(defclass api:file-writer ()
  ((output-stream
    :type stream
    :accessor output-stream)
   (file-header
    :type api:file-header
    :reader api:file-header
    :accessor file-header
    :documentation "File header.")
   (wrote-header-p
    :type boolean
    :initform nil
    :accessor wrote-header-p))
  (:documentation
   "A writer for an avro object container file."))

(defmethod initialize-instance :after
    ((instance api:file-writer)
     &key
       (output (error "Must supply OUTPUT"))
       (meta (error "Must supply META"))
       (sync (make-instance 'api:sync)))
  (with-accessors
        ((output-stream output-stream)
         (file-header file-header))
      instance
    (setf output-stream output
          file-header (make-instance 'api:file-header :sync sync :meta meta))))

;; TODO the skip/read-block TODO is applicable to write-block, too

(declaim (ftype (function (api:file-writer (vector api:object))
                          (values api:file-block &optional))
                api:write-block))
(defun api:write-block (file-writer objects)
  "Writes OBJECTS as a block into FILE-WRITER."
  (with-accessors
        ((file-header file-header)
         (output-stream output-stream)
         (wrote-header-p wrote-header-p))
      file-writer
    (unless wrote-header-p
      (api:serialize file-header :into output-stream)
      (setf wrote-header-p t))
    (let* ((header-sync (api:sync file-header))
           (schema (api:schema file-header))
           (compress (codec->compress (api:codec file-header)))
           (file-block (objects->block objects header-sync schema compress)))
      (api:serialize file-block :into output-stream)
      file-block)))
