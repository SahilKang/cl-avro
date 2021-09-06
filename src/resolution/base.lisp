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
(defpackage #:cl-avro.resolution.base
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:shadow #:coerce)
  (:export #:coerce))
(in-package #:cl-avro.resolution.base)

(defgeneric coerce (object schema)
  (:method (object schema)
    (error "~S cannot be resolved into ~S" (schema:schema-of object) schema))

  (:method :around (object (schema symbol))
    (if (typep schema 'schema:primitive-schema)
        (call-next-method)
        (coerce object (find-class schema))))

  (:method :around (object schema)
    (if (eq (schema:schema-of object) schema)
        object
        (call-next-method)))

  (:documentation
   "use Avro Schema Resolution to coerce OBJECT into SCHEMA.

OBJECT may be recursively mutated."))
