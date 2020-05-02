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

(defparameter +empty+ #xc15d213aa4d7a795)

(defun make-fingerprint-table ()
  (map 'vector
       (lambda (i)
         (reduce (lambda (i _)
                   (declare (ignore _))
                   (logxor (ash i -1)
                           (logand +empty+
                                   (- (logand i 1)))))
                 (loop for i below 8 collect i)
                 :initial-value i))
       (loop for i below 256 collect i)))

(defparameter +table+ (make-fingerprint-table))

(defun avro-64bit-crc (bytes)
  (reduce (lambda (fp byte)
            (logxor (ash fp -8)
                    (elt +table+ (logand #xff
                                         (logxor fp byte)))))
          bytes
          :initial-value +empty+))

(defparameter *default-fingerprint-algorithm* #'avro-64bit-crc
  "Default function used by FINGERPRINT to convert a byte-vector to an integer.")

(defun fingerprint (schema &optional (algorithm *default-fingerprint-algorithm*))
  "Returns the fingerprint of the avro SCHEMA object.

The fingerprinting ALGORITHM should accept a byte-vector and return an
integer."
  (check-type schema avro-schema)
  (let* ((string (schema->json schema t))
         (bytes (babel:string-to-octets string :encoding :utf-8)))
    (funcall algorithm bytes)))
