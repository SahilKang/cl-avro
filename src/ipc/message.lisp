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
  (:import-from #:cl-avro.ipc.error
                #:message)
  (:export #:message
           #:request
           #:response
           #:errors
           #:one-way))
(in-package #:cl-avro.ipc.message)

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
    :type (or null (simple-array class (*)))
    :documentation "Error condition classes if provided, otherwise nil.")
   (effective-errors
    :type schema:union
    :documentation "Effective error union.")
   (one-way
    :initarg :one-way
    :type boolean
    :documentation "Boolean indicating if message is one-way."))
  (:metaclass closer-mop:funcallable-standard-class)
  (:default-initargs
   :request (error "Must supply REQUEST")
   :response (error "Must supply RESPONSE"))
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

(defmethod initialize-instance :around
    ((instance message)
     &rest initargs &key request (errors nil errorsp) (one-way nil one-way-p))
  (if (not errorsp)
      (setf (getf initargs :errors) nil)
      (progn
        (check-type errors (simple-array class (*)))
        (when (zerop (length errors))
          ;; the errors json field represents a union and this
          ;; implementation doesn't allow empty unions, at least for the
          ;; time being
          (error "Errors cannot be an empty array"))))
  (when one-way-p
    (check-type one-way boolean))
  (setf (getf initargs :lambda-list) (deduce-lambda-list request))
  (apply #'call-next-method instance initargs))

(declaim
 (ftype (function ((or null (simple-array class (*))))
                  (values schema:union &optional))
        deduce-effective-errors))
(defun deduce-effective-errors (errors)
  (make-instance
   'schema:union
   :schemas (if errors
                (apply #'vector 'schema:string (map 'list #'schema:schema errors))
                (vector 'schema:string))))

(schema:define-initializers message :after
    (&key)
  (with-slots (errors effective-errors response) instance
    (when (one-way instance)
      (unless (eq response 'schema:null)
        (error "one-way is true but response is not null: ~S" response))
      (when errors
        ;; union doesn't allow empty schemas...that would be a problem
        ;; only if we allow an empty json array for errors
        (error "one-way is true but there are errors declared: ~S" errors)))
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
      (let ((errors (map 'simple-vector #'schema:to-jso errors)))
        (setf initargs (nconc initargs (list "errors" errors)))))
    (multiple-value-bind (one-way one-way-p) (one-way message)
      (when one-way-p
        (let ((one-way (if one-way :true :false)))
          (setf initargs (nconc initargs (list "one-way" one-way))))))
    (when documentation
      (push documentation initargs)
      (push "doc" initargs))
    (apply #'st-json:jso initargs)))
