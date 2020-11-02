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

(declaim ((unsigned-byte 64) +empty+))
#.(defparameter +empty+ #xc15d213aa4d7a795)

(declaim ((simple-vector 256) +table+))
(defparameter +table+
  #.(map 'simple-vector
         (lambda (i)
           (loop
             repeat 8
             do (setf i (logxor (ash i -1)
                                (logand +empty+
                                        (- (logand i 1)))))
             finally (return i)))
         (loop for i below 256 collect i)))

(defun+ avro-64bit-crc (bytes)
    ((vector[byte]) (unsigned-byte 64))
  (reduce (lambda (fp byte)
            (declare ((unsigned-byte 64) fp)
                     ((unsigned-byte 8) byte))
            (let ((table-ref (svref +table+ (logand #xff (logxor fp byte)))))
              (declare ((unsigned-byte 64) table-ref))
              (logxor (ash fp -8) table-ref)))
          bytes
          :initial-value +empty+))

(defparameter *default-fingerprint-algorithm* #'avro-64bit-crc
  "Default function used by FINGERPRINT.")

(defun* fingerprint (schema &optional (algorithm *default-fingerprint-algorithm*))
    ((avro-schema &optional (function (vector[byte]) (unsigned-byte 64))) (unsigned-byte 64))
  "Return the fingerprint of avro SCHEMA under ALGORITHM."
  (declare ((function (vector[byte]) (unsigned-byte 64)) algorithm))
  (let* ((string (schema->json schema t))
         (bytes (babel:string-to-octets string :encoding :utf-8)))
    (funcall algorithm bytes)))
