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
(defpackage #:cl-avro.schema.complex.map
  (:use #:cl)
  (:shadow #:map
           #:values)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema)
  (:import-from #:genhash
                #:generic-hash-table-count
                #:generic-hash-table-p
                #:generic-hash-table-size
                #:hashclr
                #:hashmap
                #:hashref
                #:hashrem)
  (:export #:map
           #:map-object
           #:values
           #:raw-hash-table
           #:generic-hash-table-count
           #:generic-hash-table-p
           #:generic-hash-table-size
           #:hashclr
           #:hashmap
           #:hashref
           #:hashrem))
(in-package #:cl-avro.schema.complex.map)

(defclass map-object ()
  ((hash-table
    :reader raw-hash-table
    :type hash-table
    :documentation "Hash-table mapping strings to values."))
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro map schema."))

(defclass map (complex-schema)
  ((values
    :initarg :values
    :reader values
    :type schema
    :documentation "Map schema value type."))
  (:default-initargs
   :values (error "Must supply VALUES"))
  (:documentation
   "Base class for avro map schemas."))

(defmethod closer-mop:validate-superclass
    ((class map) (superclass complex-schema))
  t)

(defmethod initialize-instance :after
    ((instance map-object) &key size rehash-size rehash-threshold)
  (let ((keyword-args (list :test #'equal)))
    (when size
      (push size keyword-args)
      (push :size keyword-args))
    (when rehash-size
      (push rehash-size keyword-args)
      (push :rehash-size keyword-args))
    (when rehash-threshold
      (push rehash-threshold keyword-args)
      (push :rehash-threshold keyword-args))
    (with-slots (hash-table) instance
      (setf hash-table (apply #'make-hash-table keyword-args)))))

(defmethod initialize-instance :around
    ((instance map) &rest initargs)
  (ensure-superclass map-object)
  (apply #'call-next-method instance initargs))

(defmethod generic-hash-table-count
    ((instance map-object))
  (hash-table-count (raw-hash-table instance)))

(defmethod generic-hash-table-p
    ((instance map-object))
  (hash-table-p (raw-hash-table instance)))

(defmethod generic-hash-table-size
    ((instance map-object))
  (hash-table-size (raw-hash-table instance)))

(defmethod hashclr
    ((instance map-object))
  (prog1 instance
    (clrhash (raw-hash-table instance))))

(defmethod hashmap
    (function (instance map-object))
  (maphash function (raw-hash-table instance)))

(defmethod hashref
    ((key simple-string) (instance map-object) &optional default)
  (gethash key (raw-hash-table instance) default))

(defmethod (setf hashref)
    (value (key simple-string) (instance map-object) &optional default)
  (declare (ignore default))
  (let ((schema (values (class-of instance))))
    (unless (typep value schema)
      (error "Expected type ~S, but got ~S for ~S"
             schema (type-of value) value)))
  (setf (gethash key (raw-hash-table instance)) value))

(defmethod hashrem
    ((key simple-string) (instance map-object))
  (remhash key (raw-hash-table instance)))
