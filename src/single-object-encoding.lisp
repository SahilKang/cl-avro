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

(defparameter +marker+ #(#xc3 #x01)
  "First two bytes indicating avro single-object encoding.")

(defun write-single-object (schema object)
  "Return a byte-vector according to avro's single object encoding."
  (let ((fingerprint (make-array 8 :element-type '(unsigned-byte 8)))
        (payload (serialize nil schema object)))
    (write-little-endian (fingerprint schema #'avro-64bit-crc) fingerprint)
    (concatenate 'vector +marker+ fingerprint payload)))

(defun single-object-p (bytes)
  "Determine if BYTES adheres to avro's single object encoding."
  (and (typep bytes '(typed-vector (unsigned-byte 8)))
       (>= (length bytes) 10)
       (equalp (subseq bytes 0 2) +marker+)))

(defun read-single-object (bytes)
  "Return a two-element list: (schema-fingerprint serialized-object)."
  (unless (single-object-p bytes)
    (error "~&Not a valid avro single object: ~S" bytes))
  (let ((fingerprint (read-little-endian (subseq bytes 2 10)))
        (payload (subseq bytes 10)))
    (list fingerprint payload)))
