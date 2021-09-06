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
(defpackage #:cl-avro.schema.complex.enum
  (:use #:cl)
  (:shadow #:position)
  (:import-from #:cl-avro.schema.complex.common
                #:assert-distinct
                #:which-one
                #:default
                #:define-initializers)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass)
  (:import-from #:cl-avro.schema.complex.named
                #:named-schema
                #:name
                #:namespace
                #:fullname
                #:aliases)
  (:import-from #:cl-avro.schema.primitive
                #:int)
  (:import-from #:cl-avro.schema.complex.scalarize
                #:scalarize-class
                #:scalarize-value)
  (:export #:enum
           #:enum-object
           #:symbols
           #:default
           #:which-one
           #:name
           #:namespace
           #:fullname
           #:aliases))
(in-package #:cl-avro.schema.complex.enum)

(deftype position ()
  '(and int (integer 0)))

;;; schema

(defclass enum (named-schema)
  ((symbols
    :initarg :symbols
    :reader symbols
    :type (simple-array name (*))
    :documentation "Symbols for enum.")
   (default
    :initarg :default
    :type (or null position)
    :documentation "Position of enum default."))
  (:metaclass scalarize-class)
  (:scalarize :default)
  (:default-initargs
   :symbols (error "Must supply SYMBOLS")
   :default nil)
  (:documentation
   "Base class for avro enum schemas."))

(defmethod closer-mop:validate-superclass
    ((class enum) (superclass named-schema))
  t)

(declaim (ftype (function (t) (values name &optional)) parse-symbol))
(defun parse-symbol (symbol)
  (check-type symbol name)
  symbol)

(declaim
 (ftype (function (sequence) (values (simple-array name (*)) &optional))
        parse-symbols))
(defun parse-symbols (symbols)
  (let ((symbols (map '(simple-array name (*)) #'parse-symbol symbols)))
    (when (zerop (length symbols))
      (error "Symbols cannot be empty"))
    (assert-distinct symbols)
    symbols))

(declaim
 (ftype (function ((simple-array name (*)) t)
                  (values (or null position) &optional))
        parse-default))
(defun parse-default (symbols default)
  (when default
    (check-type default name)
    (let ((position (cl:position default symbols :test #'string=)))
      (unless position
        (error "Default ~S not found in symbols ~S" default symbols))
      position)))

(define-initializers enum :around
    (&rest initargs &key symbols default)
  (let* ((symbols (parse-symbols symbols))
         (default (parse-default symbols (scalarize-value default))))
    (setf (getf initargs :symbols) symbols
          (getf initargs :default) default))
  (ensure-superclass enum-object)
  (apply #'call-next-method instance initargs))

(defmethod default ((instance enum))
  "Return (values default position)."
  (with-slots (default symbols) instance
    (if default
        (values (elt symbols default) default)
        (values nil nil))))

;;; object

(defclass enum-object ()
  ((position
    :accessor position
    :type position
    :documentation "Position of chosen enum."))
  (:metaclass complex-schema)
  (:documentation
   "Base class of objects adhering to an avro enum schema."))

(defmethod initialize-instance :after
    ((instance enum-object) &key (enum (error "Must supply ENUM")))
  (check-type enum name)
  (let* ((symbols (symbols (class-of instance)))
         (position (cl:position enum symbols :test #'string=)))
    (declare ((simple-array name (*)) symbols))
    (unless position
      (error "Enum ~S must be one of ~S" enum symbols))
    (setf (slot-value instance 'position) position)))

(defmethod which-one ((instance enum-object))
  "Return (values enum-string position)"
  (let* ((position (position instance))
         (symbols (symbols (class-of instance)))
         (symbol (elt symbols position)))
    (declare ((simple-array name (*)) symbols))
    (values symbol position)))
