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
(defpackage #:cl-avro.io.base
  (:use #:cl)
  (:export #:serialize
           #:deserialize
           #:serialized-size
           #:serialize-into))
(in-package #:cl-avro.io.base)

(defgeneric serialized-size (object)
  (:documentation
   "Determine the number of bytes OBJECT will be when serialized."))

(defgeneric serialize-into (object vector start)
  (:documentation
   "Serialize OBJECT into VECTOR and return the number of serialized bytes."))

(defgeneric serialize (object &key &allow-other-keys)
  (:method (object &key into (start 0))
    "Serialize OBJECT into vector supplied by INTO, starting at START.

If INTO is nil, then a new vector will be allocated.

Return (values vector number-of-serialized-bytes)"
    (let* ((size (serialized-size object))
           (into (or into (make-array size :element-type '(unsigned-byte 8)))))
      (check-type into (simple-array (unsigned-byte 8) (*)))
      (check-type start (and (integer 0) fixnum))
      (when (> size (- (length into) start))
        (error "Not enough room in vector"))
      (serialize-into object into start)
      (values into size))))

(defgeneric deserialize (schema input &key &allow-other-keys)
  (:documentation
   "Deserialize an instance of SCHEMA from INPUT."))
