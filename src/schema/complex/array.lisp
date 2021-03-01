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
(defpackage #:cl-avro.schema.complex.array
  (:use #:cl)
  (:shadow #:array)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema)
  (:export #:array
           #:array-object
           #:items
           #:objects))
(in-package #:cl-avro.schema.complex.array)

(defclass array-object ()
  ((objects
    :initarg :objects
    :reader objects
    :type (simple-array schema (*))
    :documentation "Array of objects."))
  (:metaclass complex-schema)
  (:default-initargs
   :objects (error "Must supply OBJECTS"))
  (:documentation
   "Base class for objects adhering to an avro array schema."))

(defclass array (complex-schema)
  ((items
    :initarg :items
    :reader items
    :type schema
    :documentation "Array schema element type."))
  (:default-initargs
   :items (error "Must supply ITEMS"))
  (:documentation
   "Base class for avro array schemas."))

(defmethod closer-mop:validate-superclass
    ((class array) (superclass complex-schema))
  t)

;; sbcl returns a copy of the array when the return type is
;; specialized to schema instead of *
(declaim
 (ftype (function (t schema) (values (simple-array * (*)) &optional))
        parse-objects)
 (inline parse-objects))
(defun parse-objects (objects schema)
  (declare (optimize (speed 3) (safety 0)))
  (let ((objects (coerce objects `(simple-array ,schema (*)))))
    (if (subtypep (array-element-type objects) schema)
        objects
        (prog1 objects
          ;; TODO maybe coerce will do this check already
          (map nil
               (lambda (object)
                 (unless (typep object schema)
                   (error "Expected type ~S, but got ~S for ~S"
                          schema (type-of object) object)))
               objects)))))
(declaim (notinline parse-items))

(defmethod initialize-instance :around
    ((instance array-object) &key (objects nil objectsp))
  (declare (optimize (speed 3) (safety 0))
           (inline parse-objects))
  (if objectsp
      (let* ((schema (items (class-of instance)))
             (objects (parse-objects objects schema)))
        (call-next-method instance :objects objects))
      (call-next-method)))

(declaim (ftype (function (schema) (values cons &optional)) make-items-slot))
(defun make-items-slot (items)
  (list :name 'items
        :type `(simple-array ,items (*))))

(defmethod initialize-instance :around
    ((instance array) &rest initargs &key items)
  (let ((items-slot (make-items-slot items)))
    (push items-slot (getf initargs :direct-slots)))
  (ensure-superclass array-object)
  (apply #'call-next-method instance initargs))
