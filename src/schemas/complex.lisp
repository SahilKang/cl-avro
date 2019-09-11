;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defun avro-schema-p (schema)
  ;; primitve schemas are symbols, whereas complex schemas are
  ;; instances
  (typecase schema
    (symbol (member schema '(null-schema
                             boolean-schema
                             int-schema
                             long-schema
                             float-schema
                             double-schema
                             bytes-schema
                             string-schema)))
    (fixed-schema t)
    (union-schema t)
    (array-schema t)
    (map-schema t)
    (enum-schema t)
    (record-schema t)))

(deftype avro-schema () '(satisfies avro-schema-p))

(defclass named-type ()
  ((name
    :initform (error "Must supply :name string")
    :initarg :name
    :reader name
    :type avro-fullname)
   (namespace
    :initform nil
    :initarg :namespace
    :reader namespace
    :type (or null-schema avro-fullname))))

(defclass aliased-type ()
  ((aliases
    :initform nil
    :initarg :aliases
    :reader aliases
    :type (or null-schema (typed-vector avro-fullname)))))

(defclass doc-type ()
  ((doc
    :initform nil
    :initarg :doc
    :reader doc
    :type (or null-schema string-schema))))

;;; avro complex types

(defclass fixed-schema (named-type aliased-type)
  ((size
    :initform (error "Must supply :size signed 32-bit int")
    :initarg :size
    :reader size
    :type int-schema)))

(defclass union-schema ()
  ((schemas
    :initform (error "Must supply :schemas")
    :initarg :schemas
    :reader schemas
    :type (typed-vector avro-schema))))

(defclass array-schema ()
  ((item-schema
    :initform (error "Must supply :item-schema")
    :initarg :item-schema
    :reader item-schema
    :type avro-schema)))

(defclass map-schema ()
  ((value-schema
    :initform (error "Must supply :value-schema")
    :initarg :value-schema
    :reader value-schema
    :type avro-schema)))

(defclass enum-schema (named-type aliased-type doc-type)
  ((symbols
    :initform (error "Must supply :symbols vector")
    :initarg :symbols
    :reader symbols
    :type (typed-vector avro-name))
   (default
    :initform nil
    :initarg :default
    :reader default
    :type (or null-schema avro-name))))

(defclass field-schema (aliased-type doc-type)
  ((name
    :initform (error "Must supply :name string")
    :initarg :name
    :reader name
    :type avro-fullname)
   (field-type
    :initform (error "Must supply :field-type schema")
    :initarg :field-type
    :reader field-type
    :type avro-schema)
   (order
    :initform "ascending"
    :initarg :order
    :reader order
    :type (enum "ascending" "descending" "ignore"))
   (default
    :initform nil
    :initarg :default
    :reader default)))

(defclass record-schema (named-type aliased-type doc-type)
  ((field-schemas
    :initform (error "Must supply :field-schemas vector")
    :initarg :field-schemas
    :reader field-schemas
    :type (typed-vector field-schema))))
