;;; Copyright 2021-2022 Google LLC
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
(defpackage #:cl-avro.internal.ipc.message
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:mop #:cl-avro.internal.mop)))
(in-package #:cl-avro.internal.ipc.message)

(deftype errors ()
  '(simple-array class (*)))

(deftype errors? ()
  '(or null errors))

(defclass api:message
    (mop:all-or-nothing-reinitialization closer-mop:standard-generic-function)
  ((request
    :initarg :request
    :reader api:request
    :type api:record
    :documentation "Request params.")
   (response
    :initarg :response
    :reader api:response
    :type api:schema
    :documentation "Response schema.")
   (errors
    :initarg :errors
    :type errors?
    :documentation "Error condition classes if provided, otherwise nil.")
   (effective-errors
    :type api:union
    :documentation "Effective error union.")
   (one-way
    :initarg :one-way
    :type boolean
    :documentation "Boolean indicating if message is one-way."))
  (:metaclass closer-mop:funcallable-standard-class)
  (:default-initargs
   :request (error "Must supply REQUEST")
   :response (error "Must supply RESPONSE")
   :errors nil)
  (:documentation
   "Metaclass of avro ipc messages."))

(defmethod closer-mop:validate-superclass
    ((class api:message) (superclass mop:all-or-nothing-reinitialization))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:message) (superclass closer-mop:standard-generic-function))
  t)

(defmethod api:errors
    ((message api:message))
  "Return (values errors effective-errors)."
  (with-slots (errors effective-errors) message
    (values errors effective-errors)))

(defmethod api:one-way
    ((message api:message))
  "Return (values one-way one-way-provided-p)."
  (if (slot-boundp message 'one-way)
      (values (slot-value message 'one-way) t)
      (values nil nil)))

(declaim
 (ftype (function (api:record) (values list &optional)) deduce-lambda-list))
(defun deduce-lambda-list (request)
  (flet ((name (field)
           (nth-value 1 (api:name field))))
    (nconc (map 'list #'name (api:fields request))
           (list '&optional 'api:metadata))))

(defmethod initialize-instance :around
    ((instance api:message) &rest initargs &key request)
  (setf (getf initargs :lambda-list) (deduce-lambda-list request))
  (apply #'call-next-method instance initargs))

(defmethod reinitialize-instance :around
    ((instance api:message) &rest initargs)
  (if initargs
      (call-next-method)
      instance))

(declaim
 (ftype (function (errors?) (values api:union &optional))
        deduce-effective-errors))
(defun deduce-effective-errors (errors)
  (flet ((schema (error-class)
           (closer-mop:ensure-finalized error-class)
           (internal:schema (closer-mop:class-prototype error-class))))
    (let ((error-schemas (map 'list #'schema errors)))
      (make-instance 'api:union :schemas (cons 'api:string error-schemas)))))

(mop:definit ((instance api:message) :after &key)
  (with-slots (errors effective-errors response) instance
    ;; the errors json field represents a union and this
    ;; implementation doesn't allow empty unions, at least for the
    ;; time being
    (assert (if errors (plusp (length errors)) t) (errors)
            "Errors cannot be an empty array")
    (when (api:one-way instance)
      (unless (eq response 'api:null)
        (error "Message is one-way but response is not null: ~S" response))
      (when errors
        ;; union doesn't allow empty schemas so we don't have to check
        ;; length...that would be a problem only if we allow an empty
        ;; json array for errors
        (error "Message is one-way but there are errors declared: ~S" errors)))
    (setf effective-errors (deduce-effective-errors errors))))

;;; jso

(defmethod internal:write-jso
    ((message api:message) seen canonical-form-p)
  (flet ((write-jso (schema)
           (internal:write-jso schema seen canonical-form-p)))
    (let ((initargs
            (list
             "request" (map 'list #'write-jso (api:fields (api:request message)))
             "response" (write-jso (api:response message))))
          (documentation (documentation message t))
          (errors (api:errors message)))
      (when errors
        (let ((errors (map 'list #'write-jso errors)))
          (setf initargs (nconc initargs (list "errors" errors)))))
      (multiple-value-bind (one-way one-way-p)
          (api:one-way message)
        (when one-way-p
          (let ((one-way (st-json:as-json-bool one-way)))
            (setf initargs (nconc initargs (list "one-way" one-way))))))
      (when documentation
        (setf initargs (nconc initargs (list "doc" documentation))))
      (apply #'st-json:jso initargs))))
