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
(defpackage #:cl-avro.schema.complex.record.schema
  (:use #:cl)
  (:local-nicknames
   (#:primitive #:cl-avro.schema.primitive)
   (#:union #:cl-avro.schema.complex.union))
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema)
  (:import-from #:cl-avro.schema.complex.named
                #:named-schema)
  (:import-from #:cl-avro.schema.complex.record.field
                #:field
                #:name
                #:aliases)
  (:import-from #:cl-avro.schema.complex.record.notation
                #:parse-notation)
  (:shadowing-import-from #:cl-avro.schema.complex.record.field
                          #:type)
  (:export #:record
           #:record-object
           #:fields))
(in-package #:cl-avro.schema.complex.record.schema)

;;; schema

(defclass record (named-schema)
  ((fields
    :reader fields
    :type (simple-array field (*))
    :documentation "Record fields.")
   (nullable-fields
    :reader nullable-fields
    :type hash-table
    :documentation "Nullable fields."))
  (:documentation
   "Base class for avro record schemas."))

(defmethod closer-mop:validate-superclass
    ((class record) (superclass named-schema))
  t)

(defmethod closer-mop:direct-slot-definition-class
    ((class record) &key)
  (find-class 'field))

(declaim
 (ftype (function (list) (values list &optional)) add-default-initargs))
(defun add-default-initargs (slots)
  (flet ((add-initarg (slot)
           (let* ((name (string (getf slot :name)))
                  (initarg (intern name 'keyword)))
             (pushnew initarg (getf slot :initargs)))
           (flet ((add-initarg (alias)
                    (pushnew (intern alias 'keyword)
                             (getf slot :initargs))))
             (map nil #'add-initarg (getf slot :aliases)))
           slot))
    (mapcar #'add-initarg slots)))

(defmethod initialize-instance :around
    ((instance record) &rest initargs)
  (setf (getf initargs :direct-slots)
        (add-default-initargs
         (getf initargs :direct-slots)))
  (ensure-superclass record-object)
  (apply #'call-next-method instance initargs))

(defmethod reinitialize-instance :around
    ((instance record) &rest initargs)
  (setf (getf initargs :direct-slots)
        (add-default-initargs
         (getf initargs :direct-slots)))
  (ensure-superclass record-object)
  (apply #'call-next-method instance initargs))

(declaim
 (ftype (function (field) (values boolean &optional)) nullable-field-p))
(defun nullable-field-p (field)
  (let ((type (type field)))
    (or (eq type 'primitive:null)
        (and (typep type 'union:union)
             (not (null (find 'primitive:null (union:schemas type) :test #'eq)))))))

(declaim
 (ftype (function ((simple-array field (*))) (values hash-table &optional))
        find-nullable-fields))
(defun find-nullable-fields (fields)
  (let ((nullable-fields (make-hash-table :test #'eq)))
    (flet ((fill-table (field)
             (when (nullable-field-p field)
               (setf (gethash field nullable-fields) t))))
      (map nil #'fill-table fields))
    nullable-fields))

(defmethod initialize-instance :after
    ((instance record) &key)
  (with-slots (fields nullable-fields) instance
    (let ((slots (closer-mop:class-direct-slots instance)))
      (setf fields (make-array (length slots)
                               :element-type 'field
                               :initial-contents slots)
            nullable-fields (find-nullable-fields fields)))))

(defmethod reinitialize-instance :after
    ((instance record) &key)
  (with-slots (fields nullable-fields) instance
    (let ((slots (closer-mop:class-direct-slots instance)))
      (setf fields (make-array (length slots)
                               :element-type 'field
                               :initial-contents slots)
            nullable-fields (find-nullable-fields fields)))))

(defmethod parse-notation
    ((schema record) (notation hash-table))
  (let ((test (hash-table-test notation))
        initargs)
    (unless (eq test 'equal)
      (error "Expected test to be #'equal, not ~S" test))
    (maphash (lambda (name value)
               (push value initargs)
               (push (intern name 'keyword) initargs))
             notation)
    (apply #'make-instance schema initargs)))

;;; object

(defclass record-object ()
  ()
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro record schema."))

(defmethod initialize-instance :after
    ((instance record-object) &key)
  (loop
    with fields of-type (simple-array field (*)) = (fields (class-of instance))
    and nullable-fields of-type hash-table = (nullable-fields (class-of instance))

    for field of-type field across fields

    for name of-type symbol = (nth-value 1 (name field))
    for type of-type schema = (type field)
    for value = (if (slot-boundp instance name)
                    (slot-value instance name)
                    (if (gethash field nullable-fields)
                        (setf (slot-value instance name)
                              (when (typep type 'union:union)
                                ;; TODO unions should be handled
                                ;; implicitly like this everywhere
                                (make-instance type :object nil)))
                        (error "Field ~S is not optional" name)))

    unless (typep value type) do
      (error "Slot ~S has value ~S which is not of type ~S"
             name value type)))
