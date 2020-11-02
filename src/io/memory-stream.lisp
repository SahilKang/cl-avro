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

;; TODO maybe add an error :default-initarg for length
(defclass bounded-binary-input-stream (trivial-gray-streams:fundamental-binary-input-stream)
  ((length
    :type long-schema)
   (position
    :initform 0
    :type long-schema))
  (:documentation
   "A mixin for bounded binary input streams."))

(defmethod stream-element-type ((stream bounded-binary-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte :around
    ((stream bounded-binary-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (position length) stream
    (declare (long-schema position length))
    (if (>= position length)
        :eof
        (prog1 (call-next-method)
          (incf (the long-schema position))))))

(defmethod trivial-gray-streams:stream-read-sequence :around
    ((stream bounded-binary-input-stream)
     sequence
     (start integer)
     (end integer)
     &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (position length) stream
    (declare (long-schema position length start end))
    (let* ((wanted (- end start))
           (remaining (- length position))
           (min (min wanted remaining)))
      (declare (long-schema wanted remaining min))
      (call-next-method stream sequence start min)
      (incf (the long-schema position) min)
      (the long-schema (+ min start)))))


(defclass byte-vector-input-stream (bounded-binary-input-stream)
  ((bytes
    :initform (error "Must supply bytes")
    :initarg :bytes
    :type vector[byte]))
  (:documentation
   "A binary input stream backed by a vector of bytes."))

(defmethod initialize-instance :after ((stream byte-vector-input-stream) &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (bytes length) stream
    (declare (vector[byte] bytes))
    (setf length (length bytes))))

(defmethod trivial-gray-streams:stream-read-byte ((stream byte-vector-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (position bytes) stream
    (svref bytes position)))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream byte-vector-input-stream)
     (vector simple-vector)
     (start integer)
     (count integer)
     &key)
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (position bytes) stream
    (declare (long-schema position start count)
             (vector[byte] bytes))
    (let ((vector-end (+ start count))
          (bytes-end (+ position count)))
      (declare (long-schema vector-end bytes-end))
      (setf (subseq vector start vector-end) (subseq bytes position bytes-end)))))


(defclass byte-vector-output-stream (trivial-gray-streams:fundamental-binary-output-stream)
  ((bytes
    :initform (make-array 0 :element-type '(unsigned-byte 8) :adjustable t :fill-pointer 0)
    :initarg :bytes
    :type vector
    :reader bytes))
  (:documentation
   "A binary output stream backed by a vector of bytes."))

(defmethod stream-element-type ((stream byte-vector-output-stream))
  (declare (optimize (speed 3) (safety 0)))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-write-byte
    ((stream byte-vector-output-stream)
     (byte integer))
  (declare ((unsigned-byte 8) byte)
           (optimize (speed 3) (safety 0)))
  (with-slots (bytes) stream
    (vector-push-extend byte bytes))
  byte)

(defmethod trivial-gray-streams:stream-write-sequence
    ((stream byte-vector-output-stream)
     (vector simple-vector)
     (start integer)
     (end integer)
     &key &allow-other-keys)
  (declare (long-schema start end)
           (optimize (speed 3) (safety 0)))
  (with-slots (bytes) stream
    (declare (vector bytes))
    (let* ((size (array-total-size bytes))
           (fill (fill-pointer bytes))
           (wanted (the long-schema (- end start)))
           (remaining (- size fill))
           (new-fill (+ fill wanted)))
      (when (< remaining wanted)
        (setf bytes (adjust-array bytes new-fill :element-type '(unsigned-byte 8))))
      (setf (fill-pointer bytes) new-fill
            (subseq bytes fill new-fill) (subseq vector start end))))
  vector)

(defun! to-simple-vector (stream)
    ((byte-vector-output-stream) vector[byte])
  (let ((bytes (bytes stream)))
    (declare (vector bytes))
    (coerce bytes 'simple-vector)))
