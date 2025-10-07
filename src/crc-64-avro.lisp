;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.crc-64-avro
  (:use #:cl)
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:uint64
                #:vector<uint8>)
  (:import-from #:alexandria
                #:define-constant)
  (:export #:crc-64-avro))
(in-package #:cl-avro.internal.crc-64-avro)

(declaim (uint64 +empty+))
(defconstant +empty+ #xc15d213aa4d7a795)

(declaim ((simple-array uint64 (256)) +table+))
(define-constant +table+
    (map '(simple-array uint64 (256))
         (lambda (i)
           (loop
             repeat 8
             do (setf i (logxor (ash i -1)
                                (logand +empty+
                                        (- (logand i 1)))))
             finally (return i)))
         (loop for i below 256 collect i))
  :test #'equalp)

(declaim
 (ftype (function (uint64 uint8) (values uint64 &optional)) %crc-64-avro))
(defun %crc-64-avro (fp byte)
  (let ((table-ref (elt +table+ (logand #xff (logxor fp byte)))))
    (logxor (ash fp -8) table-ref)))

(declaim
 (ftype (function (vector<uint8>) (values uint64 &optional)) crc-64-avro))
(defun crc-64-avro (bytes)
  (reduce #'%crc-64-avro bytes :initial-value +empty+))
