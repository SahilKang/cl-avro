;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defgeneric schema (file-stream)
  (:documentation
   "Return avro schema used in FILE-STREAM."))

(defgeneric codec (file-stream)
  (:documentation
   "Return codec used by FILE-STREAM."))

(defgeneric metadata (file-stream)
  (:documentation
   "Return avro metadata from FILE-STREAM."))

(defgeneric sync (file-stream)
  (:documentation
   "Return 16-byte sync marker from FILE-STREAM."))

(defgeneric read-block (stream)
  (:documentation
   "Return the next avro object container block from STREAM, or :EOF.

The return value is a vector whose elements are the deserialized objects from
the block."))

(defgeneric write-block (stream block)
  (:documentation
   "Deserialize the elements of BLOCK into STREAM.

Returns the number of bytes written to the object container file."))

(defgeneric skip-block (stream)
  (:documentation
   "Skip to the next avro object container block in STREAM.

Returns NIL if there were no more blocks to skip and T otherwise."))


(defparameter +magic+
  (let ((chars (mapcar #'char-code '(#\O #\b #\j))))
    (nconc chars (list 1))))

(defclass header ()
  ((sync-marker
    :reader sync-marker
    :type (typed-vector (unsigned-byte 8)))
   (metadata
    :reader metadata
    :type hash-table)
   (schema
    :reader schema
    :type avro-schema)
   (codec
    :reader codec
    :type (enum "null" "deflate" "snappy"))))

(defmethod initialize-instance :after
    ((header header)
     &key
       (magic (error "Must supply :magic byte sequence"))
       (meta (error "Must supply :meta hash-table"))
       (sync (error "Must supply :sync byte sequence")))
  (assert-magic magic)
  (with-slots (sync-marker metadata schema codec) header
    (let ((avro.schema (get-schema meta))
          (avro.codec (get-codec meta)))
      (setf sync-marker sync
            metadata meta
            schema avro.schema
            codec avro.codec))))

(defun assert-magic (magic)
  (unless (equal +magic+ (coerce magic 'list))
    (error "~&Bad header magic ~A, expected ~A" magic +magic+)))

(defun get-schema (meta)
  (multiple-value-bind (avro.schema existsp) (gethash "avro.schema" meta)
    (unless existsp
      (error "~&Missing avro.schema in header"))
    (read-schema (babel:octets-to-string avro.schema :encoding :utf-8))))

(defun get-codec (meta)
  (multiple-value-bind (avro.codec existsp) (gethash "avro.codec" meta)
    (if existsp
        (let ((string (babel:octets-to-string avro.codec :encoding :utf-8)))
          (check-type string (enum "null" "deflate" "snappy"))
          string)
        "null")))
