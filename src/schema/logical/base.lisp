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
                #:schema
                #:scalarize-class)
  (:export #:logical-schema
           #:underlying))
(in-package #:cl-avro.schema.logical.base)

(defclass logical-schema (complex-schema)
  ((underlying
    :initarg :underlying
    :type (or schema symbol)
    :reader underlying
    :documentation "Underlying schema for logical schema."))
  (:metaclass scalarize-class)
  (:scalarize :underlying)
  (:default-initargs
   :underlying (error "Must supply UNDERLYING"))
  (:documentation
   "Base class for avro logical schemas."))

(defmethod closer-mop:validate-superclass
    ((class logical-schema) (superclass complex-schema))
  t)
