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
(defpackage #:cl-avro.ipc.message
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:shadow #:error)
  (:export #:error
           #:error-union
           #:message
           #:request
           #:response
           #:errors
           #:one-way))
(in-package #:cl-avro.ipc.message)

;;; error

(defclass error (schema:record)
  ()
  (:metaclass schema:scalarize-class))

(defmethod closer-mop:validate-superclass
    ((class error) (superclass schema:record))
  t)

;;; error-union

(defclass error-union (schema:union)
  ()
  (:metaclass schema:late-class))

(defmethod closer-mop:validate-superclass
    ((class error-union) (superclass schema:union))
  t)

(cl-avro.schema.complex:define-initializers error-union :after
    (&key)
  (flet ((assert-error (schema)
           (check-type schema error)))
    (with-slots (schema:schemas) instance
      (map nil #'assert-error schema:schemas))))

;;; message

(defclass message (closer-mop:standard-generic-function)
  ((request
    :initarg :request
    :reader request
    :type schema:record
    :documentation "Request params.")
   (response
    :initarg :response
    :reader response
    :type schema:schema
    :documentation "Response schema.")
   (errors
    :initarg :errors
    :type (or null error-union)
    :documentation "Error union if provided, otherwise nil.")
   (effective-errors
    :type schema:union
    :documentation "Effective error union.")
   (one-way
    :initarg :one-way
    :type boolean
    :documentation "Boolean indicating if message is one-way."))
  (:metaclass closer-mop:funcallable-standard-class)
  (:default-initargs
   :request (cl:error "Must supply REQUEST")
   :response (cl:error "Must supply RESPONSE"))
  (:documentation
   "Base class for avro ipc messages."))

(defmethod closer-mop:validate-superclass
    ((class message) (superclass closer-mop:standard-generic-function))
  t)

(defgeneric errors (message)
  (:method ((instance message))
    "Return (values errors effective-errors)."
    (with-slots (errors effective-errors) instance
      (values errors effective-errors))))

(defgeneric one-way (message)
  (:method ((instance message))
    "Return (values one-way one-way-provided-p)."
    (if (slot-boundp instance 'one-way)
        (values (slot-value instance 'one-way) t)
        (values nil nil))))

(declaim
 (ftype (function (schema:record) (values list &optional)) deduce-lambda-list))
(defun deduce-lambda-list (request)
  (flet ((name (field)
           (nth-value 1 (schema:name field))))
    (map 'list #'name (schema:fields request))))

(cl-avro.schema.complex:define-initializers message :around
    (&rest initargs &key request (errors nil errorsp) (one-way nil one-way-p))
  (if errorsp
      (check-type errors error-union)
      (setf (getf initargs :errors) nil))
  (when one-way-p
    (check-type one-way boolean))
  (setf (getf initargs :lambda-list) (deduce-lambda-list request))
  #+nil(cl-avro.schema.complex:ensure-superclass message-object)
  (apply #'call-next-method instance initargs))

(declaim
 (ftype (function ((or null error-union)) (values schema:union &optional))
        deduce-effective-errors))
(defun deduce-effective-errors (errors)
  (make-instance
   'avro:union
   :schemas (if errors
                (concatenate '(simple-array schema:schema (*))
                             (vector 'schema:string)
                             (schema:schemas errors))
                (vector 'avro:string))))

(cl-avro.schema.complex:define-initializers message :after
    (&key)
  (with-slots (errors effective-errors response) instance
    (when (one-way instance)
      (unless (eq response 'schema:null)
        (cl:error "one-way is true but response is not null: ~S" response))
      (when errors
        ;; union doesn't allow empty schemas...that would be a problem
        ;; only if we allow an empty json array for errors
        (cl:error "one-way is true but there are errors declared: ~S" errors)))
    (setf effective-errors (deduce-effective-errors errors))))

(defmethod schema:to-jso
    ((message message))
  (let ((initargs
          (list
           "request" (map 'vector #'schema:to-jso (schema:fields (request message)))
           "response" (schema:to-jso (response message))))
        (documentation (documentation message t))
        (errors (errors message)))
    (when errors
      (let ((errors (schema:to-jso errors)))
        (setf initargs (nconc initargs (list "errors" errors)))))
    (multiple-value-bind (one-way one-way-p) (one-way message)
      (when one-way-p
        (let ((one-way (if one-way :true :false)))
          (setf initargs (nconc initargs (list "one-way" one-way))))))
    (when documentation
      (push documentation initargs)
      (push "doc" initargs))
    (apply #'st-json:jso initargs)))
