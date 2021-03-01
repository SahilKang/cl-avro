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
   :bytes +magic+))

(defmethod initialize-instance :after
    ((instance magic) &key)
  (let ((bytes (schema:bytes instance)))
    (unless (equalp bytes +magic+)
      (error "Incorrect header magic ~S, expected ~S" bytes +magic+))))

;;; meta

(deftype codec ()
  '(member null deflate snappy bzip2 xz zstandard))

(defclass meta ()
  ((schema
    :reader schema
    :type schema:schema)
   (codec
    :reader codec
    :type codec))
  (:metaclass schema:map)
  (:values schema:bytes)
  (:default-initargs
   :map (make-hash-table :test #'equal)))

(declaim
 (ftype (function (hash-table) (values schema:schema &optional)) parse-schema))
(defun parse-schema (meta)
  (multiple-value-bind (bytes existsp)
      (gethash "avro.schema" meta)
    (unless existsp
      (error "Missing avro.schema in header meta"))
    (let ((string (babel:octets-to-string bytes :encoding :utf-8)))
      (io:deserialize 'schema:schema string))))

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
 (ftype (function (hash-table) (values codec &optional)) parse-codec))
(defun parse-codec (meta)
  (multiple-value-bind (bytes existsp)
      (gethash "avro.codec" meta)
    (if (not existsp)
        'null
        (%parse-codec bytes))))

#+nil
(defmethod initialize-instance :after
    ((instance meta)
     &key
       map
       (schema (parse-schema map) schemap)
       (codec (parse-codec map) codecp))
  (declare (ignore schema codec))
  (with-slots (schema codec schema:map) instance
    (when schemap
      (setf (gethash "avro.schema" schema:map)
            (babel:string-to-octets (io:serialize schema) :encoding :utf-8)))
    (when codecp
      (setf (gethash "avro.codec" schema:map)
            (babel:string-to-octets (string-downcase (string codec))
                                    :encoding :utf-8)))))
(defmethod initialize-instance :after
    ((instance meta)
     &key
       map
       ((:schema provided-schema) (parse-schema map) schema-provided-p)
       ((:codec provided-codec) (parse-codec map)) codec-provided-p)
  (with-slots (schema codec schema:map) instance
    (setf schema provided-schema
          codec provided-codec)
    (when schema-provided-p
      (setf (gethash "avro.schema" schema:map)
            (babel:string-to-octets (io:serialize schema) :encoding :utf-8)))
    (when codec-provided-p
      (setf (gethash "avro.codec" schema:map)
            (babel:string-to-octets (string-downcase (string codec))
                                    :encoding :utf-8)))))

;;; sync

(defclass sync ()
  ()
  (:metaclass schema:fixed)
  (:size 16)
  (:default-initargs
   :bytes (loop
            with bytes = (make-array 16 :element-type '(unsigned-byte 8))

            for i below 16
            for byte = (random 256)
            do (setf (elt bytes i) byte)

            finally
               (return bytes))))

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
