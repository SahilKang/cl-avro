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
(defpackage #:cl-avro.schema.complex.record.schema
  (:use #:cl)
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
           #:fields
           #:field
           #:name->field))
(in-package #:cl-avro.schema.complex.record.schema)

(defclass record-object ()
  ()
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro record schema."))

(defclass record (named-schema)
  ((fields
    :reader fields
    :type (simple-array field (*))
    :documentation "Record fields.")
   (name->field
    :reader name->field
    :type hash-table
    :documentation "Field name to field slot map."))
  (:documentation
   "Base class for avro record schemas."))

(defmethod closer-mop:validate-superclass
    ((class record) (superclass named-schema))
  t)

(defmethod closer-mop:direct-slot-definition-class
    ((class record) &key)
  (find-class 'field))

(defmethod initialize-instance :after
    ((instance record-object) &key)
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with fields of-type (simple-array field (*)) = (fields (class-of instance))

    for field of-type field across fields

    for name of-type symbol = (nth-value 1 (name field))
    for type of-type schema = (type field)
    for value = (if (slot-boundp instance name)
                    (slot-value instance name)
                    ;; TODO setting this to nil can cause a segfault
                    (setf (slot-value instance name) nil))

    unless (typep value type) do
      (error "Slot ~S has value ~S which is not of type ~S"
             name value type)))

(defgeneric field (record-object field-name))

(defmethod field
    ((instance record-object) (field-name simple-string))
  "Return (values field-value field-slot)."
  (declare (optimize (speed 3) (safety 0)))
  (let ((field (gethash field-name (name->field (class-of instance)))))
    (unless field
      (error "No such field named ~S" field-name))
    (let* ((name (nth-value 1 (name field)))
           (value (slot-value instance name)))
      (values value field))))

(defmethod (setf field)
    (value (instance record-object) (field-name simple-string))
  "Set FIELD-NAME to VALUE."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((field (nth-value 1 (field instance field-name)))
         (type (type field))
         (name (nth-value 1 (name field))))
    (unless (typep value type)
      (error "Expected type ~S, but got ~S for ~S"
             type (type-of value) value))
    (setf (slot-value instance name) value)))

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
 (ftype (function ((simple-array field (*))) (values hash-table &optional))
        make-name->field))
(defun make-name->field (fields)
  (let ((name->field (make-hash-table :test #'equal)))
    (labels
        ((set-if-empty (name field)
           (declare (name name)
                    (field field))
           (if (gethash name name->field)
               (error "~S already names a field" name)
               (setf (gethash name name->field) field)))
         (process-field (field)
           (set-if-empty (name field) field)
           (flet ((set-if-empty (alias)
                    (set-if-empty alias field)))
             (map nil #'set-if-empty (aliases field)))))
      (map nil #'process-field fields))
    name->field))

(defmethod initialize-instance :after
    ((instance record) &key)
  (with-slots (fields name->field) instance
    (let ((slots (closer-mop:class-direct-slots instance)))
      (setf fields (make-array (length slots)
                               :element-type 'field
                               :initial-contents slots)
            name->field (make-name->field fields)))))

(defmethod reinitialize-instance :after
    ((instance record) &key)
  (with-slots (fields name->field) instance
    (let ((slots (closer-mop:class-direct-slots instance)))
      (setf fields (make-array (length slots)
                               :element-type 'field
                               :initial-contents slots)
            name->field (make-name->field fields)))))

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
