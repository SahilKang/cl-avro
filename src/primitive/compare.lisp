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
(defpackage #:cl-avro.internal.compare
  (:use #:cl)
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:vector<uint8>
                #:ufixnum
                #:comparison)
  (:export #:compare-reals
           #:compare-byte-vectors
           #:compare-byte-streams))
(in-package #:cl-avro.internal.compare)

(declaim
 (ftype (function (real real) (values comparison &optional)) compare-reals))
(defun compare-reals (left right)
  (cond
    ((= left right) 0)
    ((< left right) -1)
    (t 1)))

(declaim
 (ftype (function (vector<uint8> vector<uint8> ufixnum ufixnum ufixnum ufixnum)
                  (values comparison ufixnum ufixnum &optional))
        compare-byte-vectors))
(defun compare-byte-vectors
    (left right left-start right-start left-end right-end)
  (loop
    for left-index from left-start below left-end
    for right-index from right-start below right-end

    for bytes-read of-type ufixnum from 1

    for left-byte of-type uint8 = (elt left left-index)
    for right-byte of-type uint8 = (elt right right-index)

    if (< left-byte right-byte)
      return (values -1 bytes-read bytes-read)
    else if (> left-byte right-byte)
           return (values 1 bytes-read bytes-read)

    finally
       (return
         (let ((left-length (- left-end left-start))
               (right-length (- right-end right-start)))
           (declare (ufixnum left-length right-length))
           (values (compare-reals left-length right-length)
                   bytes-read
                   bytes-read)))))

(declaim
 (ftype (function (stream stream ufixnum ufixnum)
                  (values comparison ufixnum ufixnum &optional))
        compare-byte-streams))
(defun compare-byte-streams (left right left-length right-length)
  (loop
    repeat (min left-length right-length)
    for bytes-read of-type ufixnum from 1

    ;; TODO deftype a stream<uint8> and let folks optimize as needed
    for left-byte of-type uint8 = (read-byte left)
    for right-byte of-type uint8 = (read-byte right)

    if (< left-byte right-byte)
      return (values -1 bytes-read bytes-read)
    else if (> left-byte right-byte)
           return (values 1 bytes-read bytes-read)

    finally
       (return
         (values (compare-reals left-length right-length)
                 bytes-read
                 bytes-read))))
