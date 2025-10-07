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
(defpackage #:cl-avro.internal.count-and-size
  (:use #:cl)
  (:local-nicknames
   (#:long #:cl-avro.internal.long))
  (:import-from #:cl-avro.internal.type
                #:ufixnum
                #:vector<uint8>)
  (:export #:from-vector
           #:from-stream))
(in-package #:cl-avro.internal.count-and-size)

(declaim
 (ftype (function (vector<uint8> ufixnum)
                  (values ufixnum (or null ufixnum) ufixnum &optional))
        from-vector))
(defun from-vector (input start)
  (multiple-value-bind (count bytes-read)
      (long:deserialize-from-vector input start)
    (declare (fixnum count))
    (if (not (minusp count))
        (values count nil bytes-read)
        (multiple-value-bind (size more-bytes-read)
            (long:deserialize-from-vector input (+ start bytes-read))
          ;; this is only ufixnum if the input is wellformed
          (declare (ufixnum size))
          (values (abs count) size (+ bytes-read more-bytes-read))))))

(declaim
 (ftype (function (stream)
                  (values ufixnum (or null ufixnum) ufixnum &optional))
        from-stream))
(defun from-stream (input)
  (multiple-value-bind (count bytes-read)
      (long:deserialize-from-stream input)
    (declare (fixnum count))
    (if (not (minusp count))
        (values count nil bytes-read)
        (multiple-value-bind (size more-bytes-read)
            (long:deserialize-from-stream input)
          (declare (ufixnum size))
          (values (abs count) size (+ bytes-read more-bytes-read))))))
