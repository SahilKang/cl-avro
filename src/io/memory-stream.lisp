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

(in-package #:cl-user)
(defpackage #:cl-avro.io.memory-stream
  (:use #:cl)
  (:shadow #:position)
  (:import-from #:cl-avro.schema
                #:bytes)
  (:export #:memory-input-stream
           #:memory-output-stream
           #:bytes
           #:position))
(in-package #:cl-avro.io.memory-stream)

;; TODO can't use fixnums; must use schema:long

;;; memory-input-stream

(deftype position ()
  '(and (integer 0) fixnum))

(defclass memory-input-stream
    (trivial-gray-streams:fundamental-binary-input-stream)
  ((bytes
    :initarg :bytes
    :reader bytes
    :type (simple-array (unsigned-byte 8) (*)))
   (position
    :initarg :position
    :reader position
    :type position))
  (:default-initargs
   :bytes (error "Must supply BYTES")
   :position 0))

(defmethod stream-element-type ((stream memory-input-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte
    ((stream memory-input-stream))
  (declare (optimize (speed 3) (safety 0)))
  (with-slots (position bytes) stream
    (declare (position position)
             ((simple-array (unsigned-byte 8) (*)) bytes))
    (if (>= position (length bytes))
        :eof
        (prog1 (elt bytes position)
          (incf (the position position))))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream memory-input-stream)
     (vector simple-array)
     (start fixnum)
     (end fixnum)
     &key)
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (with-slots (position bytes) stream
    (declare (position position)
             ((simple-array (unsigned-byte 8) (*)) bytes))
    (let* ((wanted (- end start))
           (remaining (- (length bytes) position))
           (count (min wanted remaining))
           (target-end (+ start count))
           (source-end (+ position count)))
      (declare (fixnum target-end source-end))
      (replace vector bytes
               :start1 start :end1 target-end
               :start2 position :end2 source-end)
      (incf (the position position) count)
      target-end)))

;;; memory-output-stream

;; TODO this shouldn't require a vector-push-extend
;; I should also accept a start/end boundary
(defclass memory-output-stream
    (trivial-gray-streams:fundamental-binary-output-stream)
  ((bytes
    :initarg :bytes
    :reader bytes
    :type (vector (unsigned-byte 8))))
  (:default-initargs
   :bytes (make-array 0 :element-type '(unsigned-byte 8)
                        :adjustable t :fill-pointer t)))

(defmethod stream-element-type ((stream memory-output-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-write-byte
    ((stream memory-output-stream)
     (byte integer))
  (declare (optimize (speed 3) (safety 0))
           ((unsigned-byte 8) byte))
  (with-slots (bytes) stream
    (vector-push-extend byte bytes))
  byte)

(defmethod trivial-gray-streams:stream-write-sequence
    ((stream memory-output-stream)
     (vector simple-array)
     (start fixnum)
     (end fixnum)
     &key)
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (with-slots (bytes) stream
    (declare ((vector (unsigned-byte 8)) bytes))
    (let* ((size (array-total-size bytes))
           (fill (fill-pointer bytes))
           (wanted (- end start))
           (remaining (- size fill))
           (new-fill (+ fill wanted)))
      (declare (fixnum new-fill))
      (when (< remaining wanted)
        (setf bytes (adjust-array bytes new-fill
                                  :element-type '(unsigned-byte 8))))
      (setf (fill-pointer bytes) new-fill)
      (replace bytes vector
               :start1 fill :end1 new-fill
               :start2 start :end2 end)))
  vector)
