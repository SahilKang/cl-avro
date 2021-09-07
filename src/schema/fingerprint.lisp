;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
;;; Copyright 2021 Google LLC
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
           #:crc-64-avro))
(in-package #:cl-avro.schema.fingerprint)

;;; crc-64-avro

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
        %crc-64-avro))
(defun %crc-64-avro (fp byte)
  (let ((table-ref (elt +table+ (logand #xff (logxor fp byte)))))
    (logxor (ash fp -8) table-ref)))

(deftype crc-64-avro-signature ()
  `(function ((simple-array (unsigned-byte 8) (*)))
             (values (unsigned-byte 64) &optional)))

(declaim (ftype crc-64-avro-signature crc-64-avro))
(defun crc-64-avro (bytes)
  (reduce #'%crc-64-avro bytes :initial-value +empty+))

;;; fingerprint

(deftype fingerprint-function ()
  `(or
    crc-64-avro-signature
    (function ((simple-array (unsigned-byte 8) (*)))
              (values (simple-array (unsigned-byte 8) (*)) &optional))))

(declaim (fingerprint-function *default-fingerprint-algorithm*))
(defparameter *default-fingerprint-algorithm* #'crc-64-avro
  "Default function used by FINGERPRINT.")

(declaim
 (ftype (function (schema &optional fingerprint-function)
                  (values (or (unsigned-byte 64)
                              (simple-array (unsigned-byte 8) (*)))
                          &optional))
        fingerprint))
(defun fingerprint
    (schema &optional (algorithm *default-fingerprint-algorithm*))
  "Return the fingerprint of avro SCHEMA under ALGORITHM."
  (let* ((string (with-output-to-string (stream)
                   (schema->json schema stream t)))
         (bytes (babel:string-to-octets string :encoding :utf-8)))
    (funcall algorithm bytes)))
