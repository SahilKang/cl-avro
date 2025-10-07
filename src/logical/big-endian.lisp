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
(defpackage #:cl-avro.internal.logical.big-endian
  (:use #:cl)
  (:local-nicknames
   (#:bytes #:cl-avro.internal.bytes))
  (:import-from #:cl-avro.internal.type
                #:vector<uint8>
                #:ufixnum
                #:uint8)
  (:export #:from-vector
           #:from-stream
           #:to-vector
           #:to-stream))
(in-package #:cl-avro.internal.logical.big-endian)

(deftype shift ()
  `(integer 0 ,bytes:+max-size+))

;;; from-vector

(declaim
 (ftype (function (vector<uint8> ufixnum ufixnum)
                  (values (integer 0) ufixnum &optional))
        from-vector))
(defun from-vector (input start end)
  (loop
    with integer of-type (integer 0) = 0

    for index of-type ufixnum from start below end
    for byte of-type uint8 = (elt input index)
    for shift of-type shift = (* 8 (1- (- end start))) then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return
         (values integer (- end start)))))

;;; from-stream

(declaim
 (ftype (function (stream ufixnum) (values (integer 0) ufixnum &optional))
        from-stream))
(defun from-stream (input size)
  (loop
    with integer of-type (integer 0) = 0

    repeat size
    for byte of-type uint8 = (read-byte input)
    for shift of-type shift = (* 8 (1- size)) then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return
         (values integer size))))

;;; to-vector

(declaim
 (ftype (function ((integer 0) vector<uint8> ufixnum ufixnum)
                  (values ufixnum &optional))
        to-vector))
(defun to-vector (integer into start end)
  (loop
    for index of-type ufixnum from start below end
    for shift of-type shift = (* 8 (1- (- end start))) then (- shift 8)
    for byte of-type uint8 = (logand #xff (ash integer (- shift)))

    do (setf (elt into index) byte)

    finally
       (return
         (- end start))))

;;; to-stream

(declaim
 (ftype (function ((integer 0) stream ufixnum) (values ufixnum &optional))
        to-stream))
(defun to-stream (integer into size)
  (loop
    repeat size
    for shift of-type shift = (* 8 (1- size)) then (- shift 8)
    for byte of-type uint8 = (logand #xff (ash integer (- shift)))

    do (write-byte byte into)

    finally
       (return
         size)))
