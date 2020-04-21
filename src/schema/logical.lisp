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

(defclass logical-schema ()
  ((underlying-schema
    :reader underlying-schema
    :type avro-schema
    :documentation "Underlying avro schema.")))


(defclass decimal-schema (logical-schema)
  ((underlying-schema
    :type (or (eql bytes-schema) fixed-schema))
   (scale
    :reader scale
    :type (integer 0)
    :documentation "Decimal scale.")
   (precision
    :reader precision
    :type (integer 1)
    :documentation "Decimal precision"))
  (:documentation
   "Represents an avro decimal schema."))

(defun assert-decent-precision (precision size)
  (declare (integer precision size))
  (unless (> size 0)
    (error "fixed-schema with size ~S is not greater than 0" size))
  (let ((max-precision (floor
                        (log (1- (expt 2 (1- (* 8 size))))
                             10))))
    (when (> precision max-precision)
      (error "fixed-schema with size ~S can store up to ~S digits, not ~S"
             size
             max-precision
             precision))))

(defmethod initialize-instance :after
    ((decimal-schema decimal-schema)
     &key
       (underlying-schema (error "Must supply :underlying-schema"))
       (scale 0)
       (precision (error "Must supply :precision")))
  (check-type scale (integer 0))
  (check-type precision (integer 1))
  (check-type underlying-schema (or (eql bytes-schema) fixed-schema))
  (unless (<= scale precision)
    (error "Scale ~S cannot be greater than precision ~S" scale precision))
  (when (typep underlying-schema 'fixed-schema)
    (assert-decent-precision precision (size underlying-schema)))
  (with-slots ((us underlying-schema) (s scale) (p precision)) decimal-schema
    (setf us underlying-schema
          s scale
          p precision)))
