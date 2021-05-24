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
(defpackage #:cl-avro.schema.complex.record.effective-field
  (:use #:cl)
  (:local-nicknames
   (#:field #:cl-avro.schema.complex.record.field))
  (:import-from #:cl-avro.schema.complex.base
                #:schema
                #:object)
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
  (:export #:effective-field
           #:set-default-once
           #:set-order-once
           #:set-aliases-once))
(in-package #:cl-avro.schema.complex.record.effective-field)

(defclass effective-field (field:field
                           closer-mop:standard-effective-slot-definition)
  ((field:default
    :type object)))

(declaim (ftype (function (t schema) (values t &optional)) %parse-default))
(defun %parse-default (default schema)
  (typecase schema
    (symbol
     (if (and (eq schema 'bytes)
              (stringp default))
         (babel:string-to-octets default :encoding :latin-1)
         default))
    (fixed
     (if (stringp default)
         (babel:string-to-octets default :encoding :latin-1)
         default))
    (union
     (let* ((schemas (schemas (class-of schema)))
            (first (elt schemas 0)))
       (%parse-default default first)))
    (t
     default)))

(declaim (ftype (function (t schema) (values object &optional)) parse-default))
(defun parse-default (default schema)
  (if (typep default schema)
      default
      (let ((default (%parse-default default schema)))
        (parse-notation schema default))))

(defmethod initialize-instance :around
    ((instance effective-field)
     &rest initargs
     &key
       type
       (default nil defaultp))
  (when (and (symbolp type)
             (not (typep type 'schema)))
    (let ((schema (find-class type)))
      (setf type schema
            (getf initargs :type) schema)))
  (when defaultp
    (setf (getf initargs :default) (parse-default default type)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance effective-field) &key)
  (let ((type (field:type instance)))
    (check-type type schema)))

(declaim
 (ftype (function (effective-field t) (values &optional)) set-default-once))
(defun set-default-once (field default)
  (unless (slot-boundp field 'field:default)
    (setf (slot-value field 'field:default)
          (parse-default default (field:type field))))
  (values))

(declaim
 (ftype (function (effective-field field:order) (values &optional))
        set-order-once))
(defun set-order-once (field order)
  (unless (slot-boundp field 'field:order)
    (setf (slot-value field 'field:order) order))
  (values))

(declaim
 (ftype (function (effective-field (simple-array field:name (*)))
                  (values &optional))
        set-aliases-once))
(defun set-aliases-once (field aliases)
  ;; aliases slot is always bound
  (unless (slot-value field 'field:aliases)
    (setf (slot-value field 'field:aliases) aliases))
  (values))
