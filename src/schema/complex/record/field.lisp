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
(defpackage #:cl-avro.schema.complex.record.field
  (:use #:cl)
  (:shadow #:type)
  (:import-from #:cl-avro.schema.complex.common
                #:assert-distinct
                #:default)
  (:import-from #:cl-avro.schema.complex.base
                #:schema
                #:object)
  (:import-from #:cl-avro.schema.complex.named
                #:name
                #:aliases)
  (:import-from #:cl-avro.schema.primitive
                #:bytes)
  (:import-from #:cl-avro.schema.complex.fixed
                #:fixed)
  (:import-from #:cl-avro.schema.complex.union
                #:schemas)
  (:import-from #:cl-avro.schema.complex.record.notation
                #:parse-notation)
  (:shadowing-import-from #:cl-avro.schema.complex.union
                          #:union)
  (:export #:field
           #:name
           #:aliases
           #:type
           #:default
           #:order
           #:ascending
           #:descending
           #:ignore))
(in-package #:cl-avro.schema.complex.record.field)

(deftype order ()
  '(member ascending descending ignore))

;; TODO subclass readers to return nil when unbound
(defclass field (closer-mop:standard-direct-slot-definition)
  ((aliases
    :initarg :aliases
    :reader aliases
    :type (or null (simple-array name (*)))
    :documentation "A vector of aliases if provided, otherwise nil.")
   (order
    :initarg :order
    :type order
    :documentation "Field ordering used during sorting.")
   (default
    :initarg :default
    :type t
    :documentation "Field default."))
  (:default-initargs
   :name (error "Must supply NAME")
   :type (error "Must supply TYPE"))
  (:documentation
   "Slot class for an avro record field."))

(defgeneric order (field)
  (:method ((instance field))
    "Return (values order provided-p)."
    (let* ((orderp (slot-boundp instance 'order))
           (order (if orderp
                      (slot-value instance 'order)
                      'ascending)))
      (values order orderp))))

(defmethod default ((instance field))
  "Return (values default provided-p)."
  (let* ((defaultp (slot-boundp instance 'default))
         (default (when defaultp
                    (slot-value instance 'default))))
    (values default defaultp)))

;; TODO store in name-string slot to avoid string alloc
(defmethod name ((instance field))
  "Return (values name-string slot-symbol)."
  (let ((name (closer-mop:slot-definition-name instance)))
    (declare (symbol name))
    (values (string name) name)))

(defgeneric type (field)
  (:method ((instance field))
    "Field type."
    (closer-mop:slot-definition-type instance)))

(declaim
 (ftype (function (sequence boolean)
                  (values (or null (simple-array name (*))) &optional))
        parse-aliases))
(defun parse-aliases (aliases aliasesp)
  (when aliasesp
    (let ((aliases (map '(simple-array name (*))
                        (lambda (alias)
                          (check-type alias name)
                          alias)
                        aliases)))
      (assert-distinct aliases)
      aliases)))

(declaim
 (ftype (function (simple-string) (values order &optional)) %parse-order))
(defun %parse-order (order)
  (let ((expected '("ascending" "descending" "ignore")))
    (unless (member order expected :test #'string=)
      (error "Expected order ~S to be one of ~S" order expected)))
  (nth-value 0 (find-symbol (string-upcase order))))

(declaim (ftype (function (t) (values order &optional)) parse-order))
(defun parse-order (order)
  (etypecase order
    (order order)
    (simple-string (%parse-order order))))

(defmethod initialize-instance :around
    ((instance field)
     &rest initargs
     &key
       (aliases nil aliasesp)
       (order "ascending" orderp))
  (setf (getf initargs :aliases) (parse-aliases aliases aliasesp))
  (when orderp
    (setf (getf initargs :order) (parse-order order)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance field) &key)
  (with-accessors
        ((name closer-mop:slot-definition-name)
         (initfunction closer-mop:slot-definition-initfunction)
         (initform closer-mop:slot-definition-initform)
         (allocation closer-mop:slot-definition-allocation)
         (type closer-mop:slot-definition-type))
      instance
    (check-type type (or schema symbol))
    (let ((name (string name)))
      (check-type name name))
    (when initfunction
      (error "Did not expect an initform for slot ~S: ~S" name initform))
    (unless (eq allocation :instance)
      (error "Expected :INSTANCE allocation for slot ~S, not ~S"
             name allocation))))
