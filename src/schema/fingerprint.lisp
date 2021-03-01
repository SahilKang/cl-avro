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
(defpackage #:cl-avro.schema.fingerprint
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex
                #:schema)
  (:import-from #:cl-avro.schema.io
                #:schema->json)
  (:export #:fingerprint
           #:*default-fingerprint-algorithm*
           #:crc-64-avro
           #:fingerprint64))
(in-package #:cl-avro.schema.fingerprint)

(declaim ((unsigned-byte 64) +empty+))
(defconstant +empty+ #xc15d213aa4d7a795)

(declaim ((simple-array (unsigned-byte 64) (256)) +table+))
(defconstant +table+
  (if (boundp '+table+)
      +table+
      (map '(simple-array (unsigned-byte 64) (256))
           (lambda (i)
             (loop
               repeat 8
               do (setf i (logxor (ash i -1)
                                  (logand +empty+
                                          (- (logand i 1)))))
               finally (return i)))
           (loop for i below 256 collect i))))

(declaim
 (ftype (function ((unsigned-byte 64) (unsigned-byte 8))
                  (values (unsigned-byte 64) &optional))
        %fingerprint64)
 (inline %fingerprint64))
(defun %fingerprint64 (fp byte)
  (declare (optimize (speed 3) (safety 0)))
  (let ((table-ref (elt +table+ (logand #xff (logxor fp byte)))))
    (logxor (ash fp -8) table-ref)))
(declaim (notinline %fingerprint64))

;; TODO clean this shit up
(declaim
 (ftype (function ((or schema (simple-array (unsigned-byte 8) (*))))
                  (values (unsigned-byte 64) &optional))
        fingerprint64)
 (inline fingerprint64))
(defun fingerprint64 (bytes-or-schema)
  (declare (optimize (speed 3) (safety 0))
           (inline %fingerprint64))
  (let ((bytes (if (typep bytes-or-schema 'schema)
                   (babel:string-to-octets
                    (with-output-to-string (stream)
                      (schema->json bytes-or-schema stream t))
                    :encoding :utf-8)
                   bytes-or-schema)))
    (reduce #'%fingerprint64 bytes :initial-value +empty+)))
(declaim (notinline fingerprint64))

(deftype fingerprint ()
  '(function
    ((simple-array (unsigned-byte 8) (*)))
    (values (simple-array (unsigned-byte 8) (*)) &optional)))

(declaim
 (ftype (function ((unsigned-byte 64))
                  (values (simple-array (unsigned-byte 8) (8)) &optional))
        to-little-endian)
 (inline to-little-endian))
(defun to-little-endian (number)
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with bytes = (make-array 8 :element-type '(unsigned-byte 8))

    for i below 8
    for byte of-type (unsigned-byte 8) = (logand #xff (ash number (* i -8)))
    do (setf (elt bytes i) byte)

    finally
       (return bytes)))
(declaim (notinline to-little-endian))

;; TODO use better safety if this is external
(declaim
 (ftype fingerprint crc-64-avro)
 (inline crc-64-avro))
(defun crc-64-avro (bytes)
  (declare (optimize (speed 3) (safety 0))
           (inline fingerprint64 to-little-endian))
  (to-little-endian
   (fingerprint64 bytes)))
(declaim (notinline crc-64-avro))

(defparameter *default-fingerprint-algorithm* #'crc-64-avro
  "Default function used by FINGERPRINT.")

(declaim
 (ftype (function (schema &optional fingerprint)
                  (values (simple-array (unsigned-byte 8) (*)) &optional))
        fingerprint)
 (inline fingerprint))
(defun fingerprint
    (schema &optional (algorithm *default-fingerprint-algorithm*))
  "Return the fingerprint of avro SCHEMA under ALGORITHM."
  (declare (optimize (speed 3) (safety 3)))
  (locally
      (declare (optimize (speed 3) (safety 0)))
    (let* ((string (with-output-to-string (stream)
                     (schema->json schema stream t)))
           (bytes (babel:string-to-octets string :encoding :utf-8)))
      (funcall algorithm bytes))))
(declaim (notinline fingerprint))
