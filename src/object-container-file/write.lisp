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

(defparameter +meta-schema+
  (json->schema
   "{\"type\": \"map\", \"values\": \"bytes\"}"))

(defun assert-sync (sync)
  (check-type sync (typed-vector (unsigned-byte 8)))
  (unless (= (length sync) 16)
    (error "~&Sync is not a 16 byte vector: ~A" sync)))

(defun make-random-sync ()
  (loop
     with sync = (make-array 16 :element-type '(unsigned-byte 8) :fill-pointer 0)
     repeat 16
     for byte = (random 256)
     do (vector-push byte sync)
     finally (return sync)))

(defclass file-output-stream (fundamental-binary-output-stream)
  ((output-stream
    :type stream)
   (header
    :type header)
   (wrote-header-p
    :initform nil
    :type boolean)))

(defmethod initialize-instance :after
    ((file-output-stream file-output-stream)
     &key
       (stream-or-vector (error "Must supply :stream-or-vector."))
       (schema (error "Must provide avro :schema."))
       (codec "null")
       (sync nil syncp)
       (meta (make-hash-table :test #'equal)))
  "SCHEMA and CODEC override avro.schema and avro.codec values in META."
  (check-type codec (enum "null" "deflate" "snappy"))
  (check-type stream-or-vector (or stream vector))
  (unless syncp
    (setf sync (make-random-sync)))
  (assert-sync sync)
  (setf (gethash "avro.schema" meta) (babel:string-to-octets
                                      (schema->json schema) :encoding :utf-8)
        (gethash "avro.codec" meta) (babel:string-to-octets codec :encoding :utf-8))
  (unless (validp +meta-schema+ meta)
    (error "~&Meta schema is not valid for an avro file."))
  (with-slots (output-stream header) file-output-stream
    (setf header (make-instance 'header
                                :magic (coerce +magic+ 'vector)
                                :meta meta
                                :sync sync)
          output-stream (if (typep stream-or-vector 'vector)
                            (make-instance 'output-stream :bytes stream-or-vector)
                            stream-or-vector))))

(defmethod schema ((file-output-stream file-output-stream))
  (with-slots (header) file-output-stream
    (schema header)))

(defmethod codec ((file-output-stream file-output-stream))
  (with-slots (header) file-output-stream
    (codec header)))

(defmethod metadata ((file-output-stream file-output-stream))
  (with-slots (header) file-output-stream
    (metadata header)))

(defmethod sync ((file-output-stream file-output-stream))
  (with-slots (header) file-output-stream
    (sync-marker header)))

(defmethod write-block ((stream file-output-stream) (block sequence))
  (with-slots (wrote-header-p header output-stream) stream
    (let ((block-count (length block))
          (bytes (serialize-then-compress header block)))
      (unless wrote-header-p
        (serialize output-stream
                   +header-schema+
                   (list (coerce +magic+ 'vector) (metadata header) (sync-marker header)))
        (setf wrote-header-p t))
      (serialize output-stream 'long-schema block-count)
      (serialize output-stream 'long-schema (length bytes))
      ;; optimize these loops with write-sequence
      (loop for byte across bytes do (write-byte byte output-stream))
      (loop for byte across (sync-marker header) do (write-byte byte output-stream))
      (length bytes))))

(defun serialize-then-compress (header block)
  (let ((output-stream (make-instance 'output-stream)))
    (loop
       with schema = (schema header)

       for i below (length block)
       for object = (elt block i)
       do (serialize output-stream schema object))
    (compress (codec header) (bytes output-stream))))

(defun compress (codec bytes)
  (cond
    ((string= codec "null") bytes)
    ((string= codec "deflate")
     (salza2:compress-data bytes 'salza2:deflate-compressor))
    (t (error "~&Codec not supported: ~A" codec))))
