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
(defpackage #:cl-avro.internal.file.reader
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro))
  (:import-from #:cl-avro.internal.type
                #:uint8)
  (:import-from #:cl-avro.internal.file.block
                #:block->objects
                #:codec->decompress))
(in-package #:cl-avro.internal.file.reader)

;;; file-reader

(deftype %input-stream ()
  '(or flexi-streams:flexi-input-stream flexi-streams:in-memory-input-stream))

(defclass api:file-reader ()
  ((input-stream
    :type %input-stream
    :accessor input-stream)
   (file-header
    :type api:file-header
    :reader api:file-header
    :accessor file-header)
   (file-block
    :type (or null api:file-block)
    :accessor file-block))
  (:documentation
   "A reader for an avro object container file."))

(declaim (ftype (function (t) (values %input-stream &optional)) parse-input))
(defun parse-input (input)
  (if (streamp input)
      (flexi-streams:make-flexi-stream input :element-type 'uint8)
      (flexi-streams:make-in-memory-input-stream input)))

(defmethod initialize-instance :after
    ((instance api:file-reader) &key (input (error "Must supply INPUT")))
  (with-accessors
        ((input-stream input-stream)
         (file-header file-header)
         (file-block file-block))
      instance
    (setf input-stream (parse-input input)
          file-header (api:deserialize 'api:file-header input-stream)
          file-block (api:deserialize 'api:file-block input-stream))))

(declaim
 (ftype (function (api:file-reader) (values &optional)) set-next-file-block))
(defun set-next-file-block (file-reader)
  (with-accessors
        ((input-stream input-stream)
         (file-block file-block))
      file-reader
    (setf file-block
          (when (flexi-streams:peek-byte input-stream nil nil nil)
            (api:deserialize 'api:file-block input-stream))))
  (values))

;; TODO both skip-block and read-block are applicable to array-reader
;; and map-reader, so they should be generic-functions instead

(declaim
 (ftype (function (api:file-reader) (values (or null api:file-block) &optional))
        api:skip-block))
(defun api:skip-block (file-reader)
  (prog1 (file-block file-reader)
    (set-next-file-block file-reader)))

(declaim
 (ftype (function (api:file-reader)
                  (values (or null (simple-array api:object (*))) &optional))
        api:read-block))
(defun api:read-block (file-reader)
  (let ((file-header (file-header file-reader))
        (file-block (api:skip-block file-reader)))
    (when file-block
      (let ((header-sync (api:raw (api:sync file-header)))
            (schema (api:schema file-header))
            (decompress (codec->decompress (api:codec file-header))))
        (block->objects file-block header-sync schema decompress)))))
