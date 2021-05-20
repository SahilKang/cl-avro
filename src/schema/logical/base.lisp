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
(defpackage #:cl-avro.schema.logical.base
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex
                #:complex-schema
                #:schema)
  (:export #:logical-schema
           #:underlying))
(in-package #:cl-avro.schema.logical.base)

(defclass effective-slot (closer-mop:standard-effective-slot-definition)
  ((type
    :type schema)))

(defclass logical-schema (complex-schema)
  ((underlying
    :initarg :underlying
    :type (or schema symbol)
    :documentation "Underlying schema for logical schema."))
  (:default-initargs
   :underlying (error "Must supply UNDERLYING"))
  (:documentation
   "Base class for avro logical schemas."))

(defmethod closer-mop:validate-superclass
    ((class logical-schema) (superclass complex-schema))
  t)

;; this never gets called because the logcial schemas are all
;; instances of standard class...I'll need a custom metaclass for them
(defmethod closer-mop:compute-effective-slot-definition
    ((class logical-schema) (name (eql 'underlying)) slots)
  (let ((effective-slot (call-next-method)))
    (with-accessors
          ((name closer-mop:slot-definition-name)
           (initform closer-mop:slot-definition-initform)
           (initfunction closer-mop:slot-definition-initfunction)
           (type closer-mop:slot-definition-type)
           (allocation closer-mop:slot-definition-allocation)
           (initargs closer-mop:slot-definition-initargs)
           (readers closer-mop:slot-definition-readers)
           (writers closer-mop:slot-definition-writers))
        effective-slot
      (let* ((documentation (documentation effective-slot t))
             (initargs (list :name name :allocation allocation :initargs initargs
                             :readers readers :writers writers
                             :documentation documentation :type type)))
        (when initfunction
          (setf initargs (list* :initform initform :initfunction initfunction
                                initargs)))
        (apply #'make-instance 'effective-slot initargs)))))

(defmethod closer-mop:finalize-inheritance :after
    ((instance logical-schema))
  (with-slots (underlying) instance
    (when (and (symbolp underlying)
               (not (typep underlying 'schema)))
      (setf underlying (find-class underlying)))
    (check-type underlying schema)))

(defgeneric underlying (logical-schema)
  (:method ((instance logical-schema))
    (unless (closer-mop:class-finalized-p instance)
      (closer-mop:finalize-inheritance instance))
    (slot-value instance 'underlying)))
