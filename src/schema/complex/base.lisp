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
(defpackage #:cl-avro.schema.complex.base
  (:use #:cl)
  (:import-from #:cl-avro.schema.primitive
                #:primitive-schema
                #:primitive-object)
  (:import-from #:cl-avro.schema.complex.common
                #:define-initializers)
  (:export #:complex-schema
           #:complex-object
           #:schema
           #:object

           #:ensure-superclass))
(in-package #:cl-avro.schema.complex.base)

(defclass complex-object ()
  ()
  (:documentation
   "Base class for objects adhering to an avro complex schema."))

(defclass complex-schema (standard-class)
  ()
  (:documentation
   "Base class for avro complex schemas."))

(defmethod closer-mop:validate-superclass
    ((class complex-schema) (superclass standard-class))
  t)

(declaim
 (ftype (function (class class) (values boolean &optional)) superclassp))
(defun superclassp (super sub)
  "True if SUPER is an inclusive superclass of SUB."
  (nth-value 0 (subtypep sub super)))

(defmacro ensure-superclass (superclass)
  (declare (symbol superclass))
  (multiple-value-bind (initargs status)
      (find-symbol "INITARGS")
    (unless status
      (error "Could not find INITARGS"))
    `(pushnew (find-class ',superclass)
              (getf ,initargs :direct-superclasses)
              :test #'superclassp)))

(declaim
 (ftype (function (complex-schema closer-mop:slot-definition)
                  (values &optional))
        check-slot-type))
(defun check-slot-type (class slot)
  (with-accessors
        ((type closer-mop:slot-definition-type)
         (name closer-mop:slot-definition-name))
      slot
    (when (slot-boundp class name)
      (let ((value (slot-value class name)))
        (unless (typep value type)
          (error "Expected ~S to be of type ~S" value type)))))
  (values))

(declaim
 (ftype (function (complex-schema) (values &optional)) check-slot-types))
(defun check-slot-types (class)
  (flet ((check-slot-type (slot)
           (check-slot-type class slot)))
    (let ((slots (closer-mop:class-direct-slots (class-of class))))
      (map nil #'check-slot-type slots)))
  (values))

(define-initializers complex-schema :around
    (&rest initargs)
  (ensure-superclass complex-object)
  (let ((instance (apply #'call-next-method instance initargs)))
    (check-slot-types instance)
    instance))

(deftype schema ()
  "An avro schema."
  '(or primitive-schema complex-schema))

(deftype object ()
  "An object adhering to an avro schema."
  '(or primitive-object complex-object))
