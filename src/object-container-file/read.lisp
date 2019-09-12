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

(defparameter +header-schema+
  (read-schema
   "{\"type\": \"record\", \"name\": \"org.apache.avro.file.Header\",
     \"fields\": [
       {\"name\": \"magic\",
        \"type\": {\"type\": \"fixed\",
                   \"name\": \"Magic\",
                   \"size\": 4}},
       {\"name\": \"meta\",
        \"type\": {\"type\": \"map\",
                   \"values\": \"bytes\"}},
       {\"name\": \"sync\",
        \"type\": {\"type\": \"fixed\",
                   \"name\": \"Sync\",
                   \"size\": 16}}
      ]
    }"))

(defclass file-block (fundamental-binary-input-stream)
  ((block-count
    :initform (error "Must supply :block-count")
    :initarg :block-count
    :reader block-count
    :type long-schema
    :documentation "Number of objects in this block.")
   (objects-read
    :initform 0
    :type (integer 0)
    :documentation "Number of objects read from this stream.")
   (schema
    :type avro-schema
    :documentation "Schema to deserialize objects against.")
   (codec
    :type (enum "null" "deflate" "snappy"))
   (bytes
    :initform (error "Must supply :bytes")
    :initarg :bytes
    :type (typed-vector (unsigned-byte 8))
    :documentation
    "Byte vector of serialized objects in this block, compressed according to CODEC.")
   (decompressed-stream
    :initform nil
    :type (or null input-stream)
    :documentation "Used for delaying block deserialization until needed."))
  (:documentation
   "A stream representing a file data block from an avro object container file."))

(defmethod initialize-instance :after
    ((file-block file-block)
     &key
       (sync-marker (error "Must supply :sync-marker"))
       (header (error "Must supply :header")))
  (unless (equal (coerce sync-marker 'list)
                 (coerce (sync-marker header) 'list))
    (error "~&Block's sync-marker does not match header's."))
  (with-slots (schema codec decompressed-stream) file-block
    (setf schema (schema header)
          codec (codec header))))

(defun make-decompressed-stream (bytes codec)
  (cond
    ((string= codec "null")
     (make-instance 'input-stream :bytes bytes))
    ((string= codec "deflate")
     (let ((bytes (chipz:decompress nil (chipz:make-dstate :deflate) bytes)))
       (make-instance 'input-stream :bytes bytes)))
    (t (error "~&Codec not supported: ~A" codec))))

(defmethod stream-read-item ((stream file-block))
  (with-slots (block-count
               objects-read
               schema
               codec
               bytes
               decompressed-stream) stream
    (unless decompressed-stream
      (setf decompressed-stream (make-decompressed-stream bytes codec)))
    (if (= objects-read block-count)
        :eof
        (prog1 (deserialize decompressed-stream schema)
          (incf objects-read)))))

(defclass file-input-stream (fundamental-binary-input-stream)
  ((input-stream
    :initform (error "Must supply :input-stream")
    :initarg :input-stream
    :reader input-stream)
   (header
    :type header)
   (file-block
    :type (or null file-block)
    :documentation "Current file-block being streamed through."))
  (:documentation
   "A stream representing an avro object container file."))

(defmethod initialize-instance :after ((file-input-stream file-input-stream) &key)
  (with-slots (file-block input-stream header) file-input-stream
    (setf header (read-header input-stream)
          file-block (get-next-file-block input-stream header))))

(defun read-header (input-stream)
  (destructuring-bind (magic meta sync) (coerce
                                         (deserialize input-stream +header-schema+)
                                         'list)
    (make-instance 'header :magic magic :meta meta :sync sync)))

(defmethod schema ((file-input-stream file-input-stream))
  (with-slots (header) file-input-stream
    (schema header)))

(defmethod codec ((file-input-stream file-input-stream))
  (with-slots (header) file-input-stream
    (codec header)))

(defmethod metadata ((file-input-stream file-input-stream))
  (with-slots (header) file-input-stream
    (metadata header)))

(defmethod sync ((file-input-stream file-input-stream))
  (with-slots (header) file-input-stream
    (sync-marker header)))

(defun get-next-file-block (stream header)
  (let ((block-count (handler-case
                         (deserialize stream 'long-schema)
                       (end-of-file ()
                         nil))))
    (when block-count
      ;; TODO optimize the read-byte loop with read-sequence
      (let* ((object-bytes (deserialize stream 'long-schema))
             (bytes (loop repeat object-bytes collect (read-byte stream)))
             (sync (loop repeat 16 collect (read-byte stream))))
        (make-instance 'file-block
                       :bytes (coerce bytes 'vector)
                       :header header
                       :sync-marker (coerce sync 'vector)
                       :block-count block-count)))))

(defmethod read-block ((stream file-input-stream))
  (with-slots (file-block header input-stream) stream
    (if (null file-block)
        :eof
        (let ((vector (make-array (block-count file-block) :fill-pointer 0)))
          (loop
             repeat (array-dimension vector 0)

             for item = (stream-read-item file-block)
             if (eq item :eof)
             do (error 'end-of-file :stream *error-output*)
             else do (vector-push item vector))
          (setf file-block (get-next-file-block input-stream header))
          vector))))

(defmethod skip-block ((stream file-input-stream))
  (with-slots (file-block header input-stream) stream
    (unless (null file-block)
      (setf file-block (get-next-file-block input-stream header))
      t)))
