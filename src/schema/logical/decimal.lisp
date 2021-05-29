;;; Copyright 2021 Google LLC
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
(defpackage #:cl-avro.schema.logical.decimal
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.complex
                #:late-class
                #:parse-slot-value
                #:complex-schema
                #:ensure-superclass
                #:fixed
                #:size
                #:define-initializers)
  (:import-from #:cl-avro.schema.primitive
                #:bytes)
  (:export #:decimal
           #:underlying
           #:decimal-object
           #:unscaled
           #:scale
           #:precision))
(in-package #:cl-avro.schema.logical.decimal)

;;; schema

(defclass decimal (logical-schema)
  ((underlying
    :reader underlying
    :late-type (or (eql bytes) fixed))
   (scale
    :initarg :scale
    :reader scale
    :type (integer 0)
    :documentation "Decimal scale.")
   (precision
    :initarg :precision
    :reader precision
    :type (integer 1)
    :documentation "Decimal precision."))
  (:metaclass late-class)
  (:scalarize :scale :precision)
  (:default-initargs
   :precision (error "Must supply PRECISION"))
  (:documentation
   "Base class for avro decimal schemas."))

(defmethod closer-mop:validate-superclass
    ((class decimal) (superclass logical-schema))
  t)

(declaim
 (ftype (function ((integer 1)) (values (integer 1) &optional)) max-precision))
(defun max-precision (size)
  (let ((integer (1- (expt 2 (1- (* 8 size))))))
    (nth-value 0 (truncate (log integer 10)))))

(declaim
 (ftype (function ((integer 1) (integer 1)) (values &optional))
        %assert-decent-precision))
(defun %assert-decent-precision (precision size)
  (let ((max-precision (max-precision size)))
    (when (> precision max-precision)
      (error "Fixed schema with size ~S can store up to ~S digits, not ~S"
             size max-precision precision)))
  (values))

(declaim
 (ftype (function ((integer 1) (integer 0)) (values &optional))
        assert-decent-precision))
(defun assert-decent-precision (precision size)
  (when (zerop size)
    (error "Fixed schema has size 0"))
  (%assert-decent-precision precision size))

(define-initializers decimal :around
    (&rest initargs)
  (ensure-superclass decimal-object)
  (apply #'call-next-method instance initargs))

(define-initializers decimal :after
    (&key)
  (let ((precision
          (slot-value instance 'precision))
        (scale
          (if (slot-boundp instance 'scale)
              (slot-value instance 'scale)
              0)))
    (unless (<= scale precision)
      (error "Scale ~S cannot be greater than precision ~S" scale precision))))

(defmethod parse-slot-value :around
    ((class decimal) (name (eql 'underlying)) type value)
  (let ((value (call-next-method)))
    (when (typep value 'fixed)
      (assert-decent-precision (slot-value class 'precision) (size value)))
    value))

(defmethod scale
    ((instance decimal))
  "Return (values scale scale-provided-p)."
  (let* ((scalep (slot-boundp instance 'scale))
         (scale (if scalep
                    (slot-value instance 'scale)
                    0)))
    (values scale scalep)))

;;; object

(defclass decimal-object ()
  ((unscaled
    :initarg :unscaled
    :reader unscaled
    :type integer
    :documentation "Unscaled integer for decimal object."))
  (:metaclass complex-schema)
  (:default-initargs
   :unscaled (error "Must supply UNSCALED"))
  (:documentation
   "Base class for objects adhering to an avro decimal schema."))

(declaim
 (ftype (function (integer) (values (integer 1) &optional)) number-of-digits))
(defun number-of-digits (integer)
  (if (zerop integer)
      1
      (let ((abs (abs integer)))
        (nth-value 0 (ceiling (log (1+ abs) 10))))))

(defmethod initialize-instance :before
    ((instance decimal-object) &key unscaled)
  (check-type unscaled integer)
  (let ((number-of-digits (number-of-digits unscaled))
        (max-precision (precision (class-of instance))))
    (unless (<= number-of-digits max-precision)
      (error "Decimal schema with precision ~S cannot represent ~S digits"
             max-precision number-of-digits))))
