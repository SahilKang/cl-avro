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
(defpackage #:cl-avro.internal.type
  (:use #:cl)
  (:export #:uint8
           #:uint32
           #:uint64
           #:ufixnum
           #:vector<uint8>
           #:array<uint8>
           #:comparison))
(in-package #:cl-avro.internal.type)

;;; numbers

(deftype uint8 ()
  "Unsigned byte."
  '(unsigned-byte 8))

(deftype uint32 ()
  "Unsigned 32-bits."
  '(unsigned-byte 32))

(deftype uint64 ()
  "Unsigned 64-bits."
  '(unsigned-byte 64))

(deftype ufixnum ()
  "Nonnegative fixnum."
  '(and fixnum (integer 0)))

;;; vectors

(deftype vector<uint8> (&optional size)
  "Vector of bytes."
  `(vector (unsigned-byte 8) ,(if size size *)))

(deftype array<uint8> (&optional size)
  "Simple array of bytes."
  `(simple-array (unsigned-byte 8) (,(if size size *))))

;;; comparison

(deftype comparison ()
  "Return type of compare."
  '(integer -1 1))
