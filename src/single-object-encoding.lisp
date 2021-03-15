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
(defpackage #:cl-avro.single-object-encoding
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io))
  (:export #:single-object
           #:single-object-p
           #:write-single-object
           #:single-object->fingerprint
           #:deserialize-single-object))
(in-package #:cl-avro.single-object-encoding)

(declaim ((simple-array (unsigned-byte 8) (2)) +marker+))
(defconstant +marker+
  (if (boundp '+marker+)
      +marker+
      (make-array 2 :element-type '(unsigned-byte 8)
                    :initial-contents #(#xc3 #x01)))
  "First two bytes indicating avro single-object encoding.")

(declaim
 (ftype (function ((simple-array (unsigned-byte 8) (*)))
                  (values boolean &optional))
        single-object-p)
 (inline single-object-p))
(defun single-object-p (bytes)
  "Determine if BYTES adheres to avro's single-object encoding."
  (declare (optimize (speed 3) (safety 3)))
  (locally
      (declare (optimize (speed 3) (safety 0)))
    (and (>= (length bytes) 10)
         (equalp (subseq bytes 0 2) +marker+))))
(declaim (notinline single-object-p))

(deftype single-object ()
  '(and (simple-array (unsigned-byte 8) (*)) (satisfies single-object-p)))

(declaim
 (ftype
  (function (schema:object)
            (values (simple-array (unsigned-byte 8) (*)) &optional))
  write-single-object)
 (inline write-single-object))
(defun write-single-object (object)
  "Serialize OBJECT according to avro single-object encoding."
  (declare (optimize (speed 3) (safety 3))
           (inline schema:fingerprint))
  (locally
      (declare (optimize (speed 3) (safety 0)))
    (let* ((fingerprint (schema:fingerprint
                         (schema:schema-of object) #'schema:crc-64-avro))
           (payload (io:serialize object))
           (bytes (make-array
                   (+ (length +marker+) (length fingerprint) (length payload))
                   :element-type '(unsigned-byte 8))))
      (declare ((simple-array (unsigned-byte 8) (8)) fingerprint)
               ((simple-array (unsigned-byte 8) (*)) payload))
      (replace bytes +marker+)
      (replace bytes fingerprint :start1 (length +marker+))
      (replace bytes payload
               :start1 (+ (length +marker+) (length fingerprint)))
      bytes)))
(declaim (notinline write-single-object))

(declaim
 (ftype (function (single-object) (values (unsigned-byte 64) &optional))
        single-object->fingerprint)
 (inline single-object->fingerprint))
(defun single-object->fingerprint (bytes)
  (declare (optimize (speed 3) (safety 3))
           (inline cl-avro.io.primitive::little-endian->uint64))
  (locally
      (declare (optimize (speed 3) (safety 0)))
    (cl-avro.io.primitive::little-endian->uint64 bytes 2)))
(declaim (notinline single-object->fingerprint))

(declaim
 (ftype (function (schema:schema single-object)
                  (values schema:object &optional))
        deserialize-single-object)
 (inline deserialize-single-object))
(defun deserialize-single-object (schema bytes)
  (declare (optimize (speed 3) (safety 3)))
  (locally
      (declare (optimize (speed 3) (safety 0)))
    ;; to prevent copying bytes
    ;; TODO should do this throughout
    (let ((stream (make-instance 'io:memory-input-stream
                                 :bytes bytes :position 10)))
      (io:deserialize schema stream))))
(declaim (notinline deserialize-single-object))
