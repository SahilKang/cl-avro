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
(defpackage #:cl-avro.ipc.protocol.class.protocol
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:message #:cl-avro.ipc.message)
   (#:error #:cl-avro.ipc.error))
  (:import-from #:cl-avro.ipc.handshake
                #:md5)
  (:export #:protocol
           #:protocol-object
           #:messages
           #:types
           #:md5))
(in-package #:cl-avro.ipc.protocol.class.protocol)

(defclass protocol (schema:named-class)
  ((types
    :initarg :types
    :reader types
    :late-type (or null (simple-array class (*)))
    :early-type (or null (simple-array (or symbol class) (*)))
    :documentation "A vector of named types if provided, otherwise nil.")
   (messages
    :initarg :messages
    :reader messages
    :late-type (or null (simple-array message:message (*)))
    :early-type (or null (simple-array cons (*)))
    :documentation "A vector of messages if provided, otherwise nil.")
   (md5
    :reader md5
    :type md5
    :documentation "MD5 hash of protocol."))
  (:metaclass schema:late-class)
  (:documentation
   "Base class for avro protocols."))

(defmethod closer-mop:validate-superclass
    ((class protocol) (superclass schema:named-class))
  t)

(schema:define-initializers protocol :around
    (&rest initargs &key (types nil typesp) (messages nil messagesp))
  (setf (getf initargs :types) (parse-early-types types typesp)
        (getf initargs :messages) (parse-early-messages messages messagesp))
  (schema:ensure-superclass protocol-object)
  (apply #'call-next-method instance initargs))

(defmethod schema:parse-slot-value
    ((class protocol) (name (eql 'types)) type value)
  ;; this executes before the messages slot
  (with-slots (types) class
    (setf types (parse-late-types types))
    types))

(defmethod schema:parse-slot-value
    ((class protocol) (name (eql 'messages)) type value)
  (with-slots (types messages) class
    (setf messages (parse-late-messages types messages))
    messages))

(defmethod closer-mop:finalize-inheritance :after
    ((instance protocol))
  (with-slots (md5) instance
    ;; TODO not every lisp may support :utf-8 as an
    ;; external-format...so use md5sum-sequence in such cases
    (let* ((json (io:serialize instance))
           (bytes (md5:md5sum-string json :external-format :utf-8)))
      (setf md5 (make-instance 'md5 :initial-contents bytes)))))

;;; parse types

;; early

(declaim
 (ftype
  (function (t boolean)
            (values (or null (simple-array (or symbol class) (*))) &optional))
  parse-early-types))
(defun parse-early-types (types typesp)
  (when typesp
    (check-type types sequence)
    (map '(simple-array (or symbol class) (*)) #'parse-early-type types)))

(declaim
 (ftype (function (t) (values (or symbol class) &optional)) parse-early-type))
(defun parse-early-type (type)
  (check-type type (or symbol class))
  type)

;; late

(declaim
 (ftype (function ((or null (simple-array (or symbol class) (*))))
                  (values (or null (simple-array class (*))) &optional))
        parse-late-types))
(defun parse-late-types (types)
  (when types
    (map '(simple-array class (*)) #'parse-late-type types)))

(declaim
 (ftype (function ((or symbol class)) (values class &optional))
        parse-late-type))
(defun parse-late-type (class?)
  (let ((class (if (symbolp class?)
                   (find-class class?)
                   class?)))
    (unless (error:declared-rpc-error-class-p class)
      (check-type class schema:named-schema))
    class))

;;; parse messages

;; early

(declaim
 (ftype (function (t boolean)
                  (values (or null (simple-array cons (*))) &optional))
        parse-early-messages))
(defun parse-early-messages (messages messagesp)
  (when messagesp
    (check-type messages sequence)
    (let ((messages
            (map '(simple-array cons (*)) #'parse-early-message messages)))
      (assert-distinct-names messages)
      messages)))

(declaim
 (ftype (function (t) (values cons &optional)) parse-early-message))
(defun parse-early-message (message)
  (assert-plist message :name :one-way :documentation :request :response :errors)
  (check-type (getf message :name "") symbol)
  (check-type (getf message :one-way) boolean)
  (check-type (getf message :documentation "") string)
  (check-type (getf message :response "") (or schema:schema symbol))
  (assert-request message)
  (assert-errors message)
  message)

(declaim (ftype (function (cons) (values &optional)) assert-request))
(defun assert-request (message)
  (let ((request (getf message :request "")))
    (check-type request list)
    (map nil #'%assert-request request))
  (values))

(declaim (ftype (function (t) (values &optional)) %assert-request))
(defun %assert-request (request)
  (assert-plist request :name :documentation :type :default :order :aliases)
  (apply #'make-instance 'schema:field request)
  (values))

(declaim (ftype (function (cons) (values &optional)) assert-errors))
(defun assert-errors (message)
  (when (member :errors message)
    (let ((errors (getf message :errors)))
      (check-type errors cons) ; don't allow empty unions
      (map nil #'%assert-error errors)))
  (values))

(declaim (ftype (function (t) (values &optional)) %assert-error))
(defun %assert-error (error?)
  (check-type error? (or symbol class))
  (values))

(declaim
 (ftype (function ((simple-array cons (*))) (values &optional))
        assert-distinct-names))
(defun assert-distinct-names (messages)
  (flet ((name (message)
           ;; using strings to handle non-interned symbols
           (string (getf message :name))))
    (loop
      with names = (map '(simple-array string (*)) #'name messages)
      with distinct-names = (remove-duplicates names :test #'string=)

        initially
           (when (= (length names) (length distinct-names))
             (return))

      for distinct-name across distinct-names
      do (setf names (delete distinct-name names :count 1 :test #'string=))
      finally
         (error "Duplicate message names: ~S" names)))
  (values))

(declaim (ftype (function (t &rest symbol) (values &optional)) assert-plist))
(defun assert-plist (plist? &rest expected-keys)
  (check-type plist? cons)
  (unless (evenp (length plist?))
    (error "Expected a plist with an even number of elements: ~S" plist?))
  (loop
    for key in plist? by #'cddr
    unless (member key expected-keys)
      do (error "Unknown key ~S, expected one of ~S" key expected-keys)
    collect key into keys
    finally
       (loop
         for expected-key in expected-keys
         do (setf keys (delete expected-key keys :count 1))
         finally
            (when keys
              (error "Duplicate keys: ~S" keys))))
  (values))

;; late

(declaim
 (ftype
  (function ((or null (simple-array class (*)))
             (or null (simple-array cons (*))))
            (values (or null (simple-array message:message (*))) &optional))
  parse-late-messages))
(defun parse-late-messages (types messages)
  (when messages
    (flet ((parse-late-message (message)
             (parse-late-message types message)))
      (map '(simple-array message:message (*)) #'parse-late-message messages))))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   cons)
                  (values message:message &optional))
        parse-late-message))
(defun parse-late-message (types message)
  (let* ((name (getf message :name))
         (initargs
           (list
            :name name
            :request (parse-late-request types (getf message :request))
            :response (parse-late-response types (getf message :response)))))
    (when (member :one-way message)
      (push (getf message :one-way) initargs)
      (push :one-way initargs))
    (when (member :documentation message)
      (push (getf message :documentation) initargs)
      (push :documentation initargs))
    (when (member :errors message)
      (push (parse-late-errors types (getf message :errors)) initargs)
      (push :errors initargs))
    (setf (symbol-function name)
          (apply #'make-instance 'message:message initargs))))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   list)
                  (values schema:record &optional))
        parse-late-request))
(defun parse-late-request (types request)
  (flet ((%parse-late-request (request)
           (%parse-late-request types request)))
    (make-instance
     'schema:record
     :name "anonymous"
     :direct-slots (mapcar #'%parse-late-request request))))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   cons)
                  (values cons &optional))
        %parse-late-request))
(defun %parse-late-request (types request)
  (let* ((type (getf request :type))
         (schema
           (if (and (symbolp type)
                    (not (typep type 'schema:schema)))
               (find-class type)
               type)))
    (if (or (eq type schema)           ; when schema is defined inline
            (and (typep schema 'schema:schema)
                 (not (typep schema 'schema:named-schema))))
        request
        (let ((request (copy-list request)))
          (setf (getf request :type) schema)
          (unless (find schema types)
            (error "Named schema ~S not found in types ~S" schema types))
          request))))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   (or schema:schema symbol))
                  (values schema:schema &optional))
        parse-late-response))
(defun parse-late-response (types response)
  (let ((schema
          (if (and (symbolp response)
                   (not (typep response 'schema:schema)))
              (find-class response)
              response)))
    (when (and (not (eq response schema))
               (typep schema 'schema:named-schema)
               (not (find schema types)))
      (error "Named schema ~S not found in types ~S" schema types))
    schema))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   cons)
                  (values (simple-array class (*)) &optional))
        parse-late-errors))
(defun parse-late-errors (types errors)
  (flet ((parse-late-error (error)
           (parse-late-error types error)))
    (map '(simple-array class (*)) #'parse-late-error errors)))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   (or symbol class))
                  (values class &optional))
        parse-late-error))
(defun parse-late-error (types error?)
  (let ((error
          (if (symbolp error?)
              (find-class error?)
              error?)))
    (unless (or (eq error? error)
                (find error types))
      (error "Error ~S not found in types ~S" error types))
    error))
