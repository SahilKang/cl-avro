;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.complex.union
  (:use #:cl)
  (:shadow #:position
           #:union)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema
                #:object)
  (:import-from #:cl-avro.schema.complex.named
                #:named-schema
                #:valid-fullname
                #:fullname)
  (:import-from #:cl-avro.schema.primitive
                #:int)
  (:import-from #:cl-avro.schema.complex.common
                #:which-one)
  (:export #:union
           #:union-object
           #:schemas
           #:object
           #:which-one))
(in-package #:cl-avro.schema.complex.union)

;;; wrappers

(defclass wrapper-object ()
  ((wrapped-object
    :initarg :wrap
    :reader unwrap
    :documentation "Wrapped union object."))
  (:default-initargs
   :wrap (error "Must supply WRAP")))

(defclass wrapper-class (standard-class)
  ((position
    :initarg :position
    :reader position
    :type (and int (integer 0))
    :documentation "Position of chosen union schema."))
  (:default-initargs
   :position (error "Must supply POSITION")))

(defmethod closer-mop:validate-superclass
    ((class wrapper-class) (superclass standard-class))
  t)

(declaim
 (ftype (function (schema) (values cons &optional)) make-wrapped-object-slot))
(defun make-wrapped-object-slot (type)
  (list :name 'wrapped-object
        :type type))

(defmethod initialize-instance :around
    ((instance wrapper-class)
     &rest initargs
     &key (type (error "Must supply TYPE")))
  (let ((wrapped-object-slot (make-wrapped-object-slot type)))
    (push wrapped-object-slot (getf initargs :direct-slots)))
  (ensure-superclass wrapper-object)
  (apply #'call-next-method instance initargs))

;;; union schema

(defclass union-object ()
  ((wrapped-object
    :reader wrapped-object
    :type wrapper-object
    :documentation "Chosen union object."))
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro union schema."))

(defgeneric object (union-object)
  (:method ((instance union-object))
    "Return the chosen union object."
    (declare (optimize (speed 3) (safety 0)))
    (unwrap (wrapped-object instance))))

;; TODO change doc to class metaobject class for avro union schemas
(defclass union (complex-schema)
  ((schemas
    :initarg :schemas
    :reader schemas
    :type (simple-array schema (*))
    :documentation "Schemas for union.")
   (wrapper-classes
    :reader wrapper-classes
    :type (simple-array wrapper-class (*))))
  (:default-initargs
   :schemas (error "Must supply SCHEMAS"))
  (:documentation
   "Base class for avro union schemas."))

(defmethod closer-mop:validate-superclass
    ((class union) (superclass complex-schema))
  t)

(declaim
 (ftype (function (schema) (values (or symbol valid-fullname) &optional))
        schema-key)
 (inline schema-key))
(defun schema-key (schema)
  (declare (optimize (speed 3) (safety 0)))
  (if (symbolp schema)
      schema
      (if (subtypep (class-of schema) 'named-schema)
          (fullname schema)
          (type-of schema))))
(declaim (notinline schema-key))00

(defmethod which-one ((instance union-object))
  "Return (values schema-name position schema)."
  (declare (optimize (speed 3) (safety 0))
           (inline schema-key))
  (let* ((position (position (class-of (wrapped-object instance))))
         (schemas (schemas (class-of instance)))
         (schema (elt schemas position))
         (schema-name (schema-key schema)))
    (declare ((simple-array schema (*)) schemas))
    (values schema-name position schema)))

(declaim
 (ftype (function (union t) (values (or null wrapper-class) &optional))
        find-wrapper-class)
 (inline find-wrapper-class))
(defun find-wrapper-class (union object)
  (declare (optimize (speed 3) (safety 0)))
  (let* ((schemas (schemas union))
         (wrapper-classes (wrapper-classes union))
         (position (cl:position object schemas :test #'typep)))
    (declare ((simple-array schema (*)) schemas)
             ((simple-array wrapper-class (*)) wrapper-classes))
    (when position
      (elt wrapper-classes position))))
(declaim (notinline find-wrapper-class))

(declaim
 (ftype (function (union t) (values wrapper-object &optional)) wrap)
 (inline wrap))
(defun wrap (union object)
  (declare (optimize (speed 3) (safety 0))
           (inline find-wrapper-class))
  (let ((wrapper-class (find-wrapper-class union object)))
    (unless wrapper-class
      (error "Object ~S must be one of ~S" object (schemas union)))
    (make-instance wrapper-class :wrap object)))
(declaim (notinline wrap))

(defmethod initialize-instance :after
    ((instance union-object) &key (object (error "Must supply OBJECT")))
  (declare (optimize (speed 3) (safety 0))
           (inline wrap))
  (with-slots (wrapped-object) instance
    (setf wrapped-object (wrap (class-of instance) object))))

(declaim
 (ftype (function (t) (values schema &optional)) parse-schema))
(defun parse-schema (schema)
  (check-type schema schema)
  schema)

(declaim
 (ftype (function (sequence) (values (simple-array schema (*)) &optional))
        parse-schemas))
(defun parse-schemas (schemas)
  (let ((schemas (map '(simple-array schema (*)) #'parse-schema schemas)))
    (when (zerop (length schemas))
      (error "Schemas cannot be empty"))
    (let ((seen (make-hash-table :test #'equal :size (length schemas))))
      (labels
          ((assert-unique (schema)
             (let ((key (schema-key schema)))
               (if (gethash key seen)
                   (error "Duplicate ~S schema in union" key)
                   (setf (gethash key seen) t))))
           (assert-valid (schema)
             (if (subtypep (class-of schema) 'union)
                 (error "Nested union schema: ~S" schema)
                 (assert-unique schema))))
        (map nil #'assert-valid schemas)))
    schemas))

(defmethod initialize-instance :around
    ((instance union) &rest initargs &key schemas)
  (let ((schemas (parse-schemas schemas)))
    (setf (getf initargs :schemas) schemas))
  (ensure-superclass union-object)
  (apply #'call-next-method instance initargs))

(declaim
 (ftype (function ((simple-array schema (*)))
                  (values (simple-array wrapper-class (*)) &optional))
        make-wrapper-classes))
(defun make-wrapper-classes (schemas)
  (loop
    with wrapper-classes = (make-array (length schemas)
                                       :element-type 'wrapper-class)

    for schema across schemas
    for i from 0

    for wrapper-class = (make-instance 'wrapper-class :position i :type schema)
    do
       (setf (elt wrapper-classes i) wrapper-class)

    finally
       (return wrapper-classes)))

(defmethod initialize-instance :after
    ((instance union) &key)
  (with-slots (schemas wrapper-classes) instance
    (setf wrapper-classes (make-wrapper-classes schemas))))
