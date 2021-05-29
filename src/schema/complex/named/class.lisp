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
(defpackage #:cl-avro.schema.complex.named.class
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex.common
                #:define-initializers)
  (:import-from #:cl-avro.schema.complex.scalarize
                #:scalarize-class)
  (:import-from #:cl-avro.schema.complex.named.type
                #:name
                #:namespace
                #:fullname
                #:fullname->name
                #:deduce-namespace
                #:deduce-fullname)
  (:export #:named-class
           #:name
           #:namespace
           #:fullname
           #:provided-name
           #:provided-namespace
           #:deduced-name
           #:deduced-namespace))
(in-package #:cl-avro.schema.complex.named.class)

(defclass named-class (standard-class)
  ((provided-name
    :reader provided-name
    :type fullname
    :documentation "Provided class name.")
   (provided-namespace
    :initarg :namespace
    :type namespace
    :documentation "Provided class namespace.")
   (deduced-name
    :reader deduced-name
    :type name
    :documentation "Namespace unqualified name of class.")
   (deduced-namespace
    :reader deduced-namespace
    :type namespace
    :documentation "Namespace of class.")
   (fullname
    :reader fullname
    :type fullname
    :documentation "Namespace qualified name of class."))
  (:metaclass scalarize-class)
  (:scalarize :name :namespace)
  (:documentation
   "Base class for named classes."))

(defmethod closer-mop:validate-superclass
    ((class named-class) (superclass standard-class))
  t)

;; name

(defgeneric name (named-class)
  (:method ((instance named-class))
    "Returns (values deduced provided)."
    (let ((deduced-name (deduced-name instance))
          (provided-name (provided-name instance)))
      (values deduced-name provided-name))))

;; namespace

(declaim
 (ftype (function (named-class) (values boolean &optional))
        namespace-provided-p))
(defun namespace-provided-p (named-class)
  "True if namespace was provided."
  (slot-boundp named-class 'provided-namespace))

(declaim
 (ftype (function (named-class) (values namespace &optional))
        provided-namespace))
(defun provided-namespace (named-class)
  "Returns the provided-namespace."
  (when (namespace-provided-p named-class)
    (slot-value named-class 'provided-namespace)))

(defgeneric namespace (named-class)
  (:method ((instance named-class))
    "Returns (values deduced provided provided-p)."
    (let ((deduced-namespace (deduced-namespace instance))
          (provided-namespace (provided-namespace instance))
          (namespace-provided-p (namespace-provided-p instance)))
      (values deduced-namespace provided-namespace namespace-provided-p))))

;; initialization

(declaim
 (ftype (function (symbol list) (values t &optional)) %parse-name))
(defun %parse-name (name initargs)
  (let* ((initargs (cddr (member :name initargs)))
         (other-name (getf initargs :name name)))
    (if (eq name other-name)
        (string name)
        other-name)))

(declaim
 (ftype (function (t list) (values t &optional)) parse-name))
(defun parse-name (name initargs)
  (if (symbolp name)
      (%parse-name name initargs)
      name))

(define-initializers named-class :after
    (&rest initargs
     &key
     (name (or (class-name instance) (error "Must supply NAME"))))
  (let ((provided-namespace (provided-namespace instance)))
    (with-slots
          (provided-name
           deduced-name
           deduced-namespace
           fullname)
        instance
      (setf provided-name (parse-name name initargs)
            deduced-name (fullname->name provided-name)
            deduced-namespace (deduce-namespace
                               provided-name provided-namespace nil)
            fullname (deduce-fullname
                      provided-name provided-namespace nil)))))
