;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

;;; header

(declaim (fixed-schema +sync-schema+))
(defparameter +sync-schema+
  (%make-fixed-schema :name "Sync" :size 16))

(declaim (record-schema +header-schema+))
(defparameter +header-schema+
  (%make-record-schema
   :name "org.apache.avro.file.Header"
   :fields (vector
            (%make-field-schema
             :name "magic"
             :type (%make-fixed-schema :name "Magic" :size 4))
            (%make-field-schema
             :name "meta"
             :type (%make-map-schema :values 'bytes-schema))
            (%make-field-schema
             :name "sync"
             :type +sync-schema+))))

(declaim (vector[byte] +magic+))
(defparameter +magic+
  #.(vector (char-code #\O) (char-code #\b) (char-code #\j) 1))

(defenum (codec) "null" "deflate" "snappy" "bzip2" "xz" "zstandard")

(defstruct (header (:copier nil)
                   (:predicate nil))
  (sync-marker (error "Must supply SYNC-MARKER") :type vector[byte] :read-only t)
  (metadata (error "Must supply METADATA") :type hash-table :read-only t)
  (schema (error "Must supply SCHEMA") :type avro-schema :read-only t)
  (codec (error "Must supply CODEC") :type enum[codec] :read-only t))

(defun! assert-valid-magic (header)
    ((hash-table) (values))
  (let ((magic (gethash "magic" header)))
    (declare (vector[byte] magic))
    (unless (equalp magic +magic+)
      (error "Header magic ~S is not equal to expected magic ~S" magic +magic+))))

(defun! parse-metadata-schema (metadata)
    ((hash-table) avro-schema)
  (multiple-value-bind (avro.schema existsp)
      (gethash "avro.schema" metadata)
    (unless existsp
      (error "Missing avro.schema in header metadata"))
    (let ((avro.schema (babel:octets-to-string avro.schema :encoding :utf-8)))
      (json->schema avro.schema))))

(defun! parse-metadata-codec (metadata)
    ((hash-table) enum[codec])
  (multiple-value-bind (avro.codec existsp)
      (gethash "avro.codec" metadata)
    (if existsp
        (let ((avro.codec (babel:octets-to-string avro.codec :encoding :utf-8)))
          (check-type avro.codec enum[codec])
          avro.codec)
        "null")))

(defun! read-header (stream)
    ((stream) header)
  (declare (inline assert-valid-magic parse-metadata-schema parse-metadata-codec))
  (let ((header (deserialize +header-schema+ stream)))
    (assert-valid-magic header)
    (let* ((metadata (gethash "meta" header))
           (sync-marker (gethash "sync" header))
           (schema (parse-metadata-schema metadata))
           (codec (parse-metadata-codec metadata)))
      (make-header :metadata metadata
                   :schema schema
                   :codec codec
                   :sync-marker sync-marker))))

(defun! write-header (header stream)
    ((header stream) (values))
  (let ((hash-table (make-hash-table :test #'equal :size 3)))
    (setf (gethash "magic" hash-table) +magic+
          (gethash "meta" hash-table) (header-metadata header)
          (gethash "sync" hash-table) (header-sync-marker header))
    (serialize +header-schema+ hash-table stream)))

;;; file-block

(defstruct (file-block (:copier nil))
  "Represents a file data block from an avro object container file."
  (count (error "Must supply COUNT") :type long-schema :read-only t)
  (bytes (error "Must supply BYTES") :type vector[byte] :read-only t))

(defun! assert-valid-sync-marker (header sync-marker)
    ((header vector[byte]) (values))
  (let ((header-sync (header-sync-marker header)))
    (unless (equal header-sync sync-marker)
      (error "Block's sync-marker ~S does not match header's ~S" sync-marker header-sync))))

(defun! read-file-block (stream header)
    ((stream header) file-block)
  (declare (inline assert-valid-sync-marker))
  (let ((count (deserialize 'long-schema stream))
        (bytes (deserialize 'bytes-schema stream))
        (sync-marker (deserialize +sync-schema+ stream)))
    (assert-valid-sync-marker header sync-marker)
    (make-file-block :count count :bytes bytes)))

(defun! %decompress (codec bytes)
    ((enum[codec] vector[byte]) vector[byte])
  (cond
    ((string= codec "null")
     bytes)
    ((string= codec "deflate")
     (chipz:decompress nil 'chipz:deflate bytes))
    ((string= codec "bzip2")
     (chipz:decompress nil 'chipz:bzip2 bytes))
    (t (error "~S codec not supported" codec))))

(defun! decompress (header file-block)
    ((header file-block) byte-vector-input-stream)
  (declare (inline %decompress))
  (let* ((codec (header-codec header))
         (bytes (file-block-bytes file-block))
         (decompressed-bytes (%decompress codec bytes)))
    (make-instance 'byte-vector-input-stream :bytes decompressed-bytes)))

(defmethod deserialize ((header header)
                        (file-block file-block)
                        &optional writer-schema)
  "Read a vector of objects from FILE-BLOCK."
  (declare (ignore writer-schema)
           (inline decompress)
           (optimize (speed 3) (safety 0)))
  (loop
    with count = (file-block-count file-block)
    and stream = (decompress header file-block)
    and schema = (header-schema header)
    with vector = (make-array count)

    for i below count
    for item = (deserialize schema stream)
    do (setf (svref vector i) item)

    finally (return vector)))

(defun! compress (codec bytes)
    ((enum[codec] vector[byte]) vector[byte])
  (cond
    ((string= codec "null")
     bytes)
    ((string= codec "deflate")
     (salza2:compress-data bytes 'salza2:deflate-compressor))
    (t (error "~S codec not supported" codec))))

(defun! write-file-block (header count bytes stream)
    ((header long-schema vector[byte] stream) (values))
  (declare (inline compress))
  (let* ((codec (header-codec header))
         (sync-marker (header-sync-marker header))
         (compressed-bytes (compress codec bytes)))
    (serialize 'long-schema count stream)
    (serialize 'bytes-schema compressed-bytes stream)
    (serialize +sync-schema+ sync-marker stream)))

(defmethod serialize ((header header)
                      (objects simple-vector)
                      &optional stream)
  "Write OBJECTS to STREAM."
  (declare (inline write-file-block to-simple-vector)
           (optimize (speed 3) (safety 0)))
  (loop
    with count = (length objects)
    and schema = (header-schema header)
    and memory-stream = (make-instance 'byte-vector-output-stream)

    for object across objects
    do (serialize schema object memory-stream)

    finally
       (let ((bytes (to-simple-vector memory-stream)))
         (write-file-block header count bytes stream))))

;;; file-input-stream

(defstruct (file-input-stream (:constructor %make-file-input-stream)
                              (:copier nil))
  "An input stream for an avro object container file."
  (stream (error "Must supply STREAM") :type stream :read-only t)
  (header (error "Must supply HEADER") :type header :read-only t)
  (file-block (error "Must supply FILE-BLOCK") :type (or null file-block)))

(defun+ make-file-input-stream (input)
    (((or stream vector[byte])) file-input-stream)
  (declare (inline read-header read-file-block)
           (optimize (speed 3) (safety 3)))
  (let* ((stream (if (input-stream-p input)
                     input
                     (make-instance 'byte-vector-input-stream :bytes input)))
         (header (read-header stream))
         (file-block (read-file-block stream header)))
    (%make-file-input-stream :stream stream :header header :file-block file-block)))

;; TODO a mixin with just the next slot would look snazzy
(defclass peekable-binary-input-stream (trivial-gray-streams:fundamental-binary-input-stream)
  ((next
    :reader peek
    :type (or null (unsigned-byte 8)))
   (stream
    :initarg :stream
    :type stream))
  (:default-initargs
   :stream (error "Must supply STREAM")))

(defmethod initialize-instance :after ((stream peekable-binary-input-stream) &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (next (wrapped-stream stream)) stream
    (setf next (read-byte wrapped-stream nil nil))))

(defmethod stream-element-type
    ((stream peekable-binary-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte
    ((stream peekable-binary-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (next (wrapped-stream stream)) stream
    (if (null next)
        :eof
        (prog1 next
          (setf next (read-byte wrapped-stream nil nil))))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream peekable-binary-input-stream)
     (vector simple-vector)
     (start integer)
     (end integer)
     &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (next (wrapped-stream stream)) stream
    (if (null next)
        start
        (prog1 (read-sequence vector wrapped-stream :start (1+ start) :end end)
          (setf (svref vector start) next
                next (read-byte wrapped-stream nil nil))))))

(defun! end-of-stream-p (stream)
    ((peekable-binary-input-stream) boolean)
  (unless (peek stream)
    t))

(defun! read-file-block-or-nil (stream header)
    ((stream header) (or null file-block))
  (declare (inline read-file-block end-of-stream-p))
  (let ((stream (make-instance 'peekable-binary-input-stream :stream stream)))
    (unless (end-of-stream-p stream)
      (read-file-block stream header))))

(defun! set-next-file-block (stream)
    ((file-input-stream) (values))
  (declare (inline read-file-block-or-nil))
  (let* ((wrapped-stream (file-input-stream-stream stream))
         (header (file-input-stream-header stream))
         (maybe-file-block (read-file-block-or-nil wrapped-stream header)))
    (setf (file-input-stream-file-block stream) maybe-file-block)))

(defun! skip-block (file-input-stream)
    ((file-input-stream) (or null file-block))
  (declare (inline set-next-file-block)
           (optimize (speed 3) (safety 3)))
  (let ((file-block (file-input-stream-file-block file-input-stream)))
    (when file-block
      (prog1 file-block
        (set-next-file-block file-input-stream)))))

(defun! read-block (file-input-stream)
    ((file-input-stream) (values (or null simple-vector) &optional))
  (declare (inline skip-block)
           (optimize (speed 3) (safety 3)))
  (let ((header (file-input-stream-header file-input-stream))
        (file-block (skip-block file-input-stream)))
    (when file-block
      (deserialize header file-block))))

;;; file-output-stream

(defstruct (file-output-stream (:constructor %make-file-output-stream)
                               (:copier nil))
  "An output stream for an avro object container file."
  (stream (error "Must supply STREAM") :type stream :read-only t)
  (header (error "Must supply HEADER") :type header :read-only t)
  (wrote-header-p nil :type boolean))

(defun! generate-random-sync-marker ()
    (() vector[byte])
  (loop
    with sync-marker = (make-array 16)

    for i below 16
    for byte = (random 256)
    do (setf (svref sync-marker i) byte)

    finally (return sync-marker)))

(defun! overwrite-metadata (metadata schema codec)
    ((hash-table avro-schema enum[codec]) (values))
  (let ((avro.schema (babel:string-to-octets (schema->json schema) :encoding :utf-8))
        (avro.codec (babel:string-to-octets codec :encoding :utf-8)))
    (setf (gethash "avro.schema" metadata) avro.schema
          (gethash "avro.codec" metadata) avro.codec)))

(defun+ make-file-output-stream
    (&key
     (output (error "Must supply OUTPUT"))
     (schema (error "Must supply SCHEMA"))
     (codec "null")
     (sync-marker (generate-random-sync-marker))
     (metadata (make-hash-table :test #'equal)))
    ((&key
      (:output (or stream vector))
      (:schema avro-schema)
      (:codec enum[codec])
      (:sync-marker vector[byte])
      (:metadata hash-table))
     file-output-stream)
  (declare (inline generate-random-sync-marker overwrite-metadata)
           (optimize (speed 3) (safety 3)))
  "SCHEMA and CODEC will overwrite the avro.schema and avro.codec values in METADATA."
  (overwrite-metadata metadata schema codec)
  (let ((stream (if (output-stream-p output)
                    output
                    (make-instance 'byte-vector-output-stream :bytes output)))
        (header (make-header :metadata metadata
                             :schema schema
                             :codec codec
                             :sync-marker sync-marker)))
    (%make-file-output-stream :stream stream :header header)))

(defun! write-block (file-output-stream objects)
    ((file-output-stream simple-vector) (values))
  (declare (inline write-header)
           (optimize (speed 3) (safety 3)))
  (let ((stream (file-output-stream-stream file-output-stream))
        (header (file-output-stream-header file-output-stream)))
    (unless (file-output-stream-wrote-header-p file-output-stream)
      (setf (file-output-stream-wrote-header-p file-output-stream) t)
      (write-header header stream))
    (serialize header objects stream)))
