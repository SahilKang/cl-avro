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
                #:aliases
                #:default
                #:order)
  (:import-from #:cl-avro.schema.complex.record.effective-field
                #:effective-field
                #:set-default-once
                #:set-order-once
                #:set-aliases-once)
  (:import-from #:cl-avro.schema.complex.record.notation
                #:parse-notation)
  (:import-from #:cl-avro.schema.complex.common
                #:define-initializers)
  (:shadowing-import-from #:cl-avro.schema.complex.record.field
                          #:type)
  (:export #:record
           #:record-object
           #:fields))
(in-package #:cl-avro.schema.complex.record.schema)

;;; schema

(defclass record (named-schema)
  ((fields
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

(defmethod closer-mop:effective-slot-definition-class
    ((class record) &key)
  (find-class 'effective-field))

(defmethod closer-mop:compute-effective-slot-definition
    ((class record) name slots)
  (let ((effective-slot (call-next-method)))
    (dolist (slot slots effective-slot)
      (multiple-value-bind (default defaultp)
          (default slot)
        (when defaultp
          (set-default-once effective-slot default)))
      (multiple-value-bind (order orderp)
          (order slot)
        (when orderp
          (set-order-once effective-slot order)))
      (let ((aliases (aliases slot)))
        (when aliases
          (set-aliases-once effective-slot aliases))))))

(defmethod (setf closer-mop:slot-value-using-class)
    (new-value (class record) object (slot field))
  (let ((type (type slot))
        (name (name slot)))
    (unless (typep new-value type)
      (error "Expected type ~S for field ~S, but got a ~S instead: ~S"
             type name (type-of new-value) new-value)))
  (call-next-method))

(defgeneric fields (instance)
  (:method ((instance record))
    (unless (closer-mop:class-finalized-p instance)
      (closer-mop:finalize-inheritance instance))
    (slot-value instance 'fields)))

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

(define-initializers record :around
    (&rest initargs)
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

(defmethod closer-mop:finalize-inheritance :after
    ((instance record))
  (with-slots (fields nullable-fields) instance
    (let ((ordered-slot-names
            (let ((slots (closer-mop:class-direct-slots instance)))
              (make-array
               (length slots)
               :element-type 'symbol
               :initial-contents (mapcar #'closer-mop:slot-definition-name slots))))
          (slots
            (closer-mop:compute-slots instance)))
      (setf fields (sort
                    (make-array (length slots)
                                :element-type 'effective-field
                                :initial-contents slots)
                    #'<
                    :key (lambda (slot)
                           (let ((name (closer-mop:slot-definition-name slot)))
                             (position name ordered-slot-names))))
            nullable-fields (find-nullable-fields fields)))))

(defmethod parse-notation
    ((schema record) (notation hash-table))
  (loop
    with string-key-p = nil

    for key being the hash-keys of notation
      using (hash-value value)

    unless string-key-p do
      (setf string-key-p (stringp key))

    collect (intern (string key) 'keyword) into initargs
    collect value into initargs

    finally
       (when (and string-key-p
                  (plusp (hash-table-count notation))
                  (not (eq 'equal (hash-table-test notation))))
         (error "Hash-table contains a string key but test ~S is not #'equal"
                (hash-table-test notation)))
       (return (apply #'make-instance schema initargs))))

(defmethod parse-notation
    ((schema record) (notation list))
  (let ((initargs
          (if (consp (first notation))
              (alist->initargs notation)
              (plist->initargs notation))))
    (apply #'make-instance schema initargs)))

(declaim (ftype (function (cons) (values cons &optional)) alist->initargs))
(defun alist->initargs (alist)
  (loop
    for (key . value) in alist
    collect (intern (string key) 'keyword)
    collect value))

(declaim (ftype (function (list) (values list &optional)) plist->initargs))
(defun plist->initargs (plist)
  (loop
    for remaining = plist then (cddr remaining)
    while remaining

    for key = (car remaining)
    for rest = (cdr remaining)
    for value = (car rest)

    unless rest do
      (error "Odd number of key-value pairs: ~S" plist)

    collect (intern (string key) 'keyword)
    collect value))

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
