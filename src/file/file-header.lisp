;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.internal.file.header
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:array<uint8>)
  (:import-from #:alexandria
                #:define-constant))
(in-package #:cl-avro.internal.file.header)

;;; magic

(declaim ((array<uint8> 4) +magic+))
(define-constant +magic+
    (make-array 4 :element-type 'uint8
                  :initial-contents (nconc
                                     (mapcar #'char-code '(#\O #\b #\j))
                                     (list 1)))
  :test #'equalp)

(defclass api:magic ()
  ()
  (:metaclass api:fixed)
  (:size 4)
  (:name "Magic")
  (:enclosing-namespace "org.apache.avro.file")
  (:default-initargs
   :initial-contents +magic+))

(defmethod initialize-instance :after
    ((instance api:magic) &key)
  (let ((bytes (api:raw instance)))
    (assert (equalp bytes +magic+) ()
            "Incorrect header magic ~S, expected ~S" bytes +magic+)))

;;; meta

(defclass api:meta ()
  ((schema
    :initarg :schema
    :type api:schema)
   (codec
    :initarg :codec
    :type simple-string))
  (:metaclass api:map)
  (:values api:bytes))

(defmethod initialize-instance :after
    ((instance api:meta) &key)
  (when (slot-boundp instance 'schema)
    (setf (api:gethash "avro.schema" instance)
          (babel:string-to-octets
           (api:serialize (api:schema instance)) :encoding :utf-8)))
  (when (slot-boundp instance 'codec)
    (setf (api:gethash "avro.codec" instance)
          (babel:string-to-octets (api:codec instance) :encoding :utf-8))))

(declaim
 (ftype (function (api:meta) (values api:schema &optional)) parse-schema))
(defun parse-schema (meta)
  (multiple-value-bind (bytes existsp)
      (api:gethash "avro.schema" meta)
    (assert existsp () "Missing avro.schema in header meta")
    (let ((string (babel:octets-to-string bytes :encoding :utf-8)))
      (nth-value 0 (api:deserialize 'api:schema string)))))

(defmethod api:schema
    ((instance api:meta))
  (if (slot-boundp instance 'schema)
      (slot-value instance 'schema)
      (setf (slot-value instance 'schema)
            (parse-schema instance))))

(declaim (ftype (function (api:meta) (values string &optional)) parse-codec))
(defun parse-codec (meta)
  (multiple-value-bind (bytes existsp)
      (api:gethash "avro.codec" meta)
    (if (or (not existsp)
            (zerop (length bytes)))
        "null"
        (babel:octets-to-string bytes :encoding :utf-8))))

(defmethod api:codec
    ((instance api:meta))
  (if (slot-boundp instance 'codec)
      (slot-value instance 'codec)
      (setf (slot-value instance 'codec)
            (parse-codec instance))))

;;; sync

(defclass api:sync ()
  ()
  (:metaclass api:fixed)
  (:size 16)
  (:name "Sync")
  (:enclosing-namespace "org.apache.avro.file")
  (:default-initargs
   :initial-contents (loop repeat 16 collect (random 256))))

;;; file-header

(defclass api:file-header ()
  ((|magic|
    :initarg :magic
    :type api:magic
    :reader api:magic)
   (|meta|
    :initarg :meta
    :type api:meta
    :reader api:meta)
   (|sync|
    :initarg :sync
    :type api:sync
    :reader api:sync))
  (:metaclass api:record)
  (:name "org.apache.avro.file.Header")
  (:default-initargs
   :magic (make-instance 'api:magic)
   :sync (make-instance 'api:sync)))

(defmethod api:schema
    ((instance api:file-header))
  (api:schema (api:meta instance)))

(defmethod api:codec
    ((instance api:file-header))
  (api:codec (api:meta instance)))
