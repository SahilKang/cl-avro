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
(defpackage #:cl-avro.object-container-file.header
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io))
  (:shadowing-import-from #:cl-avro.schema
                          #:null
                          #:schema)
  (:export #:header
           #:magic
           #:meta
           #:sync

           #:schema
           #:codec

           #:null
           #:deflate
           #:snappy
           #:bzip2
           #:xz
           #:zstandard))
(in-package #:cl-avro.object-container-file.header)

;;; magic

(declaim ((simple-array (unsigned-byte 8) (4)) +magic+))
(defconstant +magic+
  (if (boundp '+magic+)
      +magic+
      (make-array 4 :element-type '(unsigned-byte 8)
                    :initial-contents (nconc
                                       (mapcar #'char-code '(#\O #\b #\j))
                                       (list 1)))))

(defclass magic ()
  ()
  (:metaclass schema:fixed)
  (:size 4)
  (:default-initargs
   :initial-contents +magic+))

(defmethod initialize-instance :after
    ((instance magic) &key)
  (let ((bytes (schema:raw-buffer instance)))
    (unless (equalp bytes +magic+)
      (error "Incorrect header magic ~S, expected ~S" bytes +magic+))))

;;; meta

(deftype codec ()
  '(member null deflate snappy bzip2 xz zstandard))

(defclass meta ()
  ((schema
    :initarg :schema
    :type schema:schema)
   (codec
    :initarg :codec
    :type codec))
  (:metaclass schema:map)
  (:values schema:bytes))

(defmethod initialize-instance :after
    ((instance meta) &key)
  (when (slot-boundp instance 'schema)
    (setf (schema:hashref "avro.schema" instance)
          (babel:string-to-octets
           (io:serialize (schema instance)) :encoding :utf-8)))
  (when (slot-boundp instance 'codec)
    (setf (schema:hashref "avro.codec" instance)
          (babel:string-to-octets
           (string-downcase (string (codec instance))) :encoding :utf-8))))

(declaim
 (ftype (function (schema:map-object) (values schema:schema &optional))
        parse-schema))
(defun parse-schema (meta)
  (multiple-value-bind (bytes existsp)
      (schema:hashref "avro.schema" meta)
    (unless existsp
      (error "Missing avro.schema in header meta"))
    (let ((string (babel:octets-to-string bytes :encoding :utf-8)))
      (io:deserialize 'schema:schema string))))

(defgeneric schema (meta)
  (:method ((instance meta))
    (if (slot-boundp instance 'schema)
        (slot-value instance 'schema)
        (setf (slot-value instance 'schema)
              (parse-schema instance)))))

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values codec &optional))
        %parse-codec))
(defun %parse-codec (bytes)
  (let* ((string (babel:octets-to-string bytes :encoding :utf-8))
         (symbol (find-symbol (string-upcase string)
                              'cl-avro.object-container-file.header)))
    (check-type symbol codec)
    symbol))

(declaim
 (ftype (function (schema:map-object) (values codec &optional)) parse-codec))
(defun parse-codec (meta)
  (multiple-value-bind (bytes existsp)
      (schema:hashref "avro.codec" meta)
    (if (not existsp)
        'null
        (%parse-codec bytes))))

(defgeneric codec (meta)
  (:method ((instance meta))
    (if (slot-boundp instance 'codec)
        (slot-value instance 'codec)
        (setf (slot-value instance 'codec)
              (parse-codec instance)))))

;;; sync

(defclass sync ()
  ()
  (:metaclass schema:fixed)
  (:size 16)
  (:default-initargs
   :initial-contents (loop repeat 16 collect (random 256))))

;;; header

(defclass header ()
  ((magic
    :reader magic
    :type magic)
   (meta
    :reader meta
    :type meta)
   (sync
    :reader sync
    :type sync))
  (:metaclass schema:record)
  (:default-initargs
   :magic (make-instance 'magic)
   :sync (make-instance 'sync)))

(defmethod schema ((object header))
  (schema (meta object)))

(defmethod codec ((object header))
  (codec (meta object)))
