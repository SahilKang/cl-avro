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

(defun empty-string-p (string)
  (and (stringp string)
       (zerop (length string))))

(deftype empty-string () '(satisfies empty-string-p))

(defclass named-type ()
  ((name
    :reader name
    :type avro-fullname
    :documentation "Schema name.")
   (namespace
    :type (or empty-string avro-fullname)
    :documentation "Schema namespace.")
   (namespacep
    :type boolean)))

(defmethod initialize-instance :after
    ((named-type named-type)
     &key
       (name (error "Must supply :name string"))
       (namespace "" namespacep))
  (check-type name avro-fullname)
  (check-type namespace (or empty-string avro-fullname))
  (with-slots ((n name) (ns namespace) (nsp namespacep)) named-type
    (setf n name
          ns namespace
          nsp namespacep)))

(defmethod namespace ((named-type named-type))
  "Schema namespace, returns (values namespace namespacep)."
  (with-slots (namespace namespacep) named-type
    (values namespace namespacep)))

(defclass aliased-type ()
  ((aliases
    :type (typed-vector avro-fullname)
    :documentation "Schema aliases.")
   (aliasesp
    :type boolean)))

(defmethod initialize-instance :after
    ((aliased-type aliased-type)
     &key (aliases #() aliasesp))
  (when (listp aliases)
    (setf aliases (coerce aliases 'vector)))
  (check-type aliases (typed-vector avro-fullname))
  (with-slots ((a aliases) (ap aliasesp)) aliased-type
    (setf a aliases
          ap aliasesp)))

(defmethod aliases ((aliased-type aliased-type))
  "Schema aliases, returns (values aliases aliasesp)."
  (with-slots (aliases aliasesp) aliased-type
    (values aliases aliasesp)))

(defclass doc-type ()
  ((doc
    :type string-schema
    :documentation "Schema doc.")
   (docp
    :type boolean)))

(defmethod initialize-instance :after
    ((doc-type doc-type)
     &key (doc "" docp))
  (check-type doc string-schema)
  (with-slots ((d doc) (dp docp)) doc-type
    (setf d doc
          dp docp)))

(defmethod doc ((doc-type doc-type))
  "Schema doc, returns (values doc docp)."
  (with-slots (doc docp) doc-type
    (values doc docp)))

;;; avro complex types

(defclass fixed-schema (named-type aliased-type)
  ((size
    :reader size
    :type int-schema
    :documentation "Size of fixed-schema."))
  (:documentation
   "Represents an avro fixed schema."))

(defmethod initialize-instance :after
    ((fixed-schema fixed-schema)
     &key (size (error "Must supply :size signed 32-bit int")))
  (check-type size int-schema)
  (setf (slot-value fixed-schema 'size) size))

(defclass union-schema ()
  ((schemas
    :reader schemas
    :type (typed-vector avro-schema)
    :documentation "Constituent schemas of union-schema."))
  (:documentation
   "Represents an avro union schema."))

(defmethod initialize-instance :after
    ((union-schema union-schema)
     &key (schemas (error "Must supply :schemas")))
  (when (listp schemas)
    (setf schemas (coerce schemas 'vector)))
  (check-type schemas (typed-vector avro-schema))
  (setf (slot-value union-schema 'schemas) schemas))

(defclass array-schema ()
  ((item-schema
    :reader item-schema
    :type avro-schema
    :documentation "Schema of array items."))
  (:documentation
   "Represents an avro array schema."))

(defmethod initialize-instance :after
    ((array-schema array-schema)
     &key (item-schema (error "Must supply :item-schema")))
  (check-type item-schema avro-schema)
  (setf (slot-value array-schema 'item-schema) item-schema))

(defclass map-schema ()
  ((value-schema
    :reader value-schema
    :type avro-schema
    :documentation "Schema of map values."))
  (:documentation
   "Represents an avro map schema."))

(defmethod initialize-instance :after
    ((map-schema map-schema)
     &key (value-schema (error "Must supply :value-schema")))
  (check-type value-schema avro-schema)
  (setf (slot-value map-schema 'value-schema) value-schema))

(defclass enum-schema (named-type aliased-type doc-type)
  ((symbols
    :reader symbols
    :type (typed-vector avro-name)
    :documentation "Enum values.")
   (default
    :type (or null-schema avro-name)
    :documentation "Default enum value."))
  (:documentation
   "Represents an avro enum schema."))

(defmethod initialize-instance :after
    ((enum-schema enum-schema)
     &key
       (symbols (error "Must supply :symbols vector"))
       (default nil))
  (when (listp symbols)
    (setf symbols (coerce symbols 'vector)))
  (check-type symbols (typed-vector avro-name))
  (unless (= (length symbols)
             (length (remove-duplicates symbols :test #'string=)))
    (error "~&Enum symbols must be unique: ~S" symbols))
  (check-type default (or null avro-name))
  (when (and default (not (position default symbols :test #'string=)))
    (error "~&Default enum value ~S is not in symbols vector ~S" default symbols))
  (with-slots ((s symbols) (d default)) enum-schema
    (setf s symbols
          d default)))

(defmethod default ((enum-schema enum-schema))
  "Default enum value, returns (values default defaultp)."
  (with-slots (default) enum-schema
    (values default (when default t))))

(defclass field-schema (aliased-type doc-type)
  ((name
    :reader name
    :type avro-name
    :documentation "Name of record field.")
   (field-type
    :reader field-type
    :type avro-schema
    :documentation "Schema of record field.")
   (order
    :type (enum "ascending" "descending" "ignore")
    :documentation "Order of record field.")
   (orderp
    :type boolean)
   (default)
   (defaultp
    :type boolean))
  (:documentation
   "Represents an avro record's field schema."))

(defmethod initialize-instance :after
    ((field-schema field-schema)
     &key
       (name (error "Must supply :name string"))
       (field-type (error "Must supply :field-type schema"))
       (order "ascending" orderp)
       (default nil defaultp))
  (check-type name avro-name)
  (check-type field-type avro-schema)
  (check-type order (enum "ascending" "descending" "ignore"))
  (with-slots ((n name)
               (ft field-type)
               (o order)
               (op orderp)
               (d default)
               (dp defaultp)) field-schema
    (setf n name
          ft field-type
          o order
          op orderp
          d default
          dp defaultp)))

(defmethod order ((field-schema field-schema))
  "Order of record field, returns (values order orderp)."
  (with-slots (order orderp) field-schema
    (values order orderp)))

(defmethod default ((field-schema field-schema))
  "Default value for record field, returns (values default defaultp)."
  (with-slots (default defaultp) field-schema
    (values default defaultp)))

(defclass record-schema (named-type aliased-type doc-type)
  ((field-schemas
    :reader field-schemas
    :type (typed-vector field-schema)
    :documentation "Schemas for record fields."))
  (:documentation
   "Represents an avro record schema."))

(defmethod initialize-instance :after
    ((record-schema record-schema)
     &key (field-schemas (error "Must supply :field-schemas vector")))
  (when (listp field-schemas)
    (setf field-schemas (coerce field-schemas 'vector)))
  (check-type field-schemas (typed-vector field-schema))
  (setf (slot-value record-schema 'field-schemas) field-schemas))
