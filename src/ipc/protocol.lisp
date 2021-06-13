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
(defpackage #:cl-avro.ipc.protocol
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:message #:cl-avro.ipc.message)
   (#:transceiver #:cl-avro.ipc.transceiver)
   (#:handshake #:cl-avro.ipc.handshake)
   (#:framing #:cl-avro.ipc.framing)))
(in-package #:cl-avro.ipc.protocol)

;;; protocol

(deftype named-schema ()
  'schema:named-schema)

(deftype named-schema? ()
  '(or symbol named-schema))

(deftype vector<named-schema> ()
  '(simple-array named-schema (*)))

(deftype vector<named-schema?> ()
  '(simple-array named-schema? (*)))

(defclass protocol (schema:named-class)
  ((types
    :initarg :types
    :reader types
    :late-type (or null vector<named-schema>)
    :early-type (or null vector<named-schema?>)
    :documentation "A vector of named types if provided, otherwise nil.")
   (messages
    :initarg :messages
    :reader messages
    :late-type (or null (simple-array message:message (*)))
    :early-type (or null (simple-array cons (*)))
    :documentation "A vector of messages if provided, otherwise nil.")
   (md5
    :reader md5
    :type handshake:md5
    :documentation "MD5 hash of protocol."))
  (:metaclass schema:late-class)
  (:documentation
   "Base class for avro protocols."))

(defmethod closer-mop:validate-superclass
    ((class protocol) (superclass schema:named-class))
  t)

(declaim
 (ftype (function (t) (values named-schema? &optional)) parse-early-type))
(defun parse-early-type (type)
  (check-type type named-schema?)
  type)

(declaim
 (ftype (function (t boolean)
                  (values (or null vector<named-schema?>) &optional))
        parse-early-types))
(defun parse-early-types (types typesp)
  (when typesp
    (check-type types sequence)
    (map 'vector<named-schema?> #'parse-early-type types)))

;; plist

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

;; request

(declaim (ftype (function (t) (values &optional)) %assert-request))
(defun %assert-request (request)
  (assert-plist request :name :documentation :type :default :order :aliases)
  (apply #'make-instance 'schema:field request)
  (values))

(declaim (ftype (function (cons) (values &optional)) assert-request))
(defun assert-request (message)
  (let ((request (getf message :request "")))
    (check-type request list)
    (map nil #'%assert-request request))
  (values))

;; errors

(declaim (ftype (function (t) (values &optional)) %assert-error))
(defun %assert-error (error?)
  (check-type error? (or message:error symbol))
  (values))

(declaim (ftype (function (cons) (values &optional)) assert-errors))
(defun assert-errors (message)
  (when (member :errors message)
    (let ((errors (getf message :errors)))
      (check-type errors cons) ; don't allow empty unions
      (map nil #'%assert-error errors)))
  (values))

;; messages

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

(cl-avro.schema.complex:define-initializers protocol :around
    (&rest initargs &key (types nil typesp) (messages nil messagesp))
  (setf (getf initargs :types) (parse-early-types types typesp)
        (getf initargs :messages) (parse-early-messages messages messagesp))
  (cl-avro.schema.complex:ensure-superclass protocol-object)
  (apply #'call-next-method instance initargs))

;; late-types

(declaim
 (ftype (function (named-schema?) (values named-schema &optional))
        parse-late-type))
(defun parse-late-type (schema?)
  (let ((schema
          (if (and (symbolp schema?)
                   (not (typep schema? 'schema:schema)))
              (find-class schema?)
              schema?)))
    (check-type schema named-schema)
    (let ((expected '(schema:record schema:enum schema:fixed message:error)))
      (unless (member (type-of schema) expected)
        (error "Schema ~S is not one of ~S" schema expected)))
    schema))

(declaim
 (ftype (function ((or null vector<named-schema?>))
                  (values (or null vector<named-schema>) &optional))
        parse-late-types))
(defun parse-late-types (types)
  (when types
    (map 'vector<named-schema> #'parse-late-type types)))

(defmethod schema:parse-slot-value
    ((class protocol) (name (eql 'types)) type value)
  ;; this should execute before the messages slot
  (with-slots (types) class
    (setf types (parse-late-types types))
    types))

;; late-messages

(declaim
 (ftype (function ((or null vector<named-schema>)
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
            (not (typep schema 'named-schema)))
        request
        (let ((request (copy-list request)))
          (setf (getf request :type) schema)
          (unless (find schema types)
            (error "Named schema ~S not found in types ~S" schema types))
          request))))

(declaim
 (ftype (function ((or null vector<named-schema>)
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
 (ftype (function ((or null vector<named-schema>)
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
               (typep schema 'named-schema)
               (not (find schema types)))
      (error "Named schema ~S not found in types ~S" schema types))
    schema))

(declaim
 (ftype (function ((or null vector<named-schema>)
                   (or message:error symbol))
                  (values message:error &optional))
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

(declaim
 (ftype (function ((or null vector<named-schema>)
                   cons)
                  (values message:error-union &optional))
        parse-late-errors))
(defun parse-late-errors (types errors)
  (flet ((parse-late-error (error)
           (parse-late-error types error)))
    (make-instance
     'message:error-union
     :schemas (mapcar #'parse-late-error errors))))

(declaim
 (ftype (function ((or null vector<named-schema>)
                   cons)
                  (values message:message &optional))
        parse-late-message))
(defun parse-late-message (types message)
  (let ((initargs
          (list
           :name (getf message :name)
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
    (apply #'make-instance 'message:message initargs)))

(declaim
 (ftype
  (function ((or null vector<named-schema>)
             (or null (simple-array cons (*))))
            (values (or null (simple-array message:message (*))) &optional))
  parse-late-messages))
(defun parse-late-messages (types messages)
  (when messages
    (flet ((parse-late-message (message)
             (parse-late-message types message)))
      (map '(simple-array message:message (*)) #'parse-late-message messages))))

(defmethod schema:parse-slot-value
    ((class protocol) (name (eql 'messages)) type value)
  (with-slots (types messages md5) class
    (setf messages (parse-late-messages types messages))
    messages))

;; md5

(defmethod closer-mop:finalize-inheritance :after
    ((instance protocol))
  (with-slots (md5) instance
    ;; TODO not every lisp may support :utf-8 as an
    ;; external-format...so use md5sum-sequence in such cases
    (let* ((json (io:serialize instance))
           (bytes (md5:md5sum-string json :external-format :utf-8)))
      (setf md5 (make-instance 'handshake:md5 :initial-contents bytes)))))

(defmethod schema:to-jso
    ((protocol protocol))
  (let ((initargs (list "protocol" (nth-value 1 (schema:name protocol))))
        (documentation (documentation protocol t))
        (types (types protocol))
        (messages (messages protocol)))
    (multiple-value-bind (_ namespace namespacep)
        (schema:namespace protocol)
      (declare (ignore _))
      (when namespacep
        (setf initargs (nconc initargs (list "namespace" namespace)))))
    (when documentation
      (setf initargs (nconc initargs (list "doc" documentation))))
    (when types
      (let ((types (map 'vector #'schema:to-jso types)))
        (setf initargs (nconc initargs (list "types" types)))))
    (when messages
      (loop
        for message across messages
        for name = (string (closer-mop:generic-function-name message))
        for jso = (schema:to-jso message)

        collect name into messages-initargs
        collect jso into messages-initargs

        finally
           (let ((messages (apply #'st-json:jso messages-initargs)))
             (setf initargs (nconc initargs (list "messages" messages))))))
    (apply #'st-json:jso initargs)))

(defmethod io:serialize
    ((protocol protocol) &key)
  "Return json string representation of PROTOCOL."
  (let ((schema:*seen* (make-hash-table :test #'eq)))
    (st-json:write-json-to-string (schema:to-jso protocol))))

;; TODO implement deserialize

;;; protocol-object

;; TODO maybe move the client caching to the protocol object, instead
(defclass protocol-object ()
  ((transceiver
    :initarg :transceiver
    :reader transceiver
    :type (or transceiver:client transceiver:server)
    :documentation "Protocol transceiver.")
   (methods
    ;; I guess server can have :around methods, or an array of
    ;; functions which parse out the args and generic-function
    :reader methods
    :type (or null (simple-array standard-method (*)))))
  (:default-initargs
   :transceiver (error "Must supply TRANSCEIVER.")))

(defgeneric make-methods (protocol transceiver messages)
  (:method (protocol transceiver (messages null))
    nil))

(defmethod initialize-instance :after
    ((instance protocol-object) &key)
  (with-slots (transceiver methods) instance
    (let* ((protocol (class-of instance))
           (messages (messages protocol)))
      (when (typep transceiver 'transceiver:client)
        (with-accessors
              ((server-hash transceiver:server-hash)
               (server-protocol transceiver:server-protocol))
            transceiver
          (setf server-hash (md5 protocol)
                server-protocol protocol)))
      (setf methods (make-methods protocol transceiver messages)))))

(defmethod make-methods
    ((protocol protocol)
     (client transceiver:stateless-client)
     (messages simple-array))
  (declare ((simple-array message:message (*)) messages))
  (flet ((make-stateless-method (message)
           (make-stateless-method protocol client message)))
    (map '(simple-array standard-method (*)) #'make-stateless-method messages)))

(declaim
 (ftype (function (protocol transceiver:stateless-client message:message)
                  (values standard-method &optional))
        make-stateless-method))
(defun make-stateless-method (protocol client message)
  (let ((message-name (closer-mop:generic-function-name message))
        (lambda-list (closer-mop:generic-function-lambda-list message))
        (request-schema (message:request message))
        (response-schema (message:response message))
        (errors-schema (nth-value 1 (message:errors message))))
    (let* ((body
             `(lambda ,lambda-list
                (let* ((metadata (parse-metadata ,lambda-list))
                       (parameters (parameters-record ,request-schema ,lambda-list))
                       (response-stream
                         (perform-handshake
                          ,protocol ,client ,message-name parameters metadata)))
                  ,@(unless (message:one-way message)
                      `((process-response
                         response-stream
                         (find-server-message ,message-name ,client)
                         ,response-schema
                         ,errors-schema))))))
           (method-lambda
             (closer-mop:make-method-lambda
              message (closer-mop:class-prototype (find-class 'standard-method))
              body nil))
           (documentation
             "Some auto-generated documentation would be nice.")
           (specializers
             (map 'list
                  (lambda (field)
                    (let ((schema (schema:type field)))
                      (if (symbolp schema)
                          (closer-mop:intern-eql-specializer schema)
                          schema)))
                  (schema:fields request-schema))))
      (make-instance
       'standard-method
       :lambda-list lambda-list
       :specializers specializers
       :function (compile nil method-lambda)
       :documentation documentation))))

(declaim
 (ftype (function (framing:input-stream
                   message:message
                   schema:schema
                   schema:union)
                  (values schema:object handshake:map<bytes> &optional))
        process-response))
(defun process-response
    (response-stream server-message response-schema errors-schema)
  (let ((metadata (io:deserialize 'handshake:map<bytes> response-stream))
        (errorp (eq 'schema:true
                    (io:deserialize 'schema:boolean response-stream))))
    (if errorp
        ;; TODO maybe include metadata in signalled error
        (signal-error
         (io:deserialize
          (nth-value 1 (message:errors server-message))
          response-stream
          :reader-schema errors-schema))
        (values
         (io:deserialize
          (message:response server-message)
          response-stream
          :reader-schema response-schema)
         metadata))))

(declaim
 (ftype (function (schema:union) (values &optional)) signal-error))
(defun signal-error (errors)
  (let ((error (schema:object errors)))
    (if (stringp error)
        ;; TODO use some string condition
        (error "Avro server returned error: ~S" error)
        ;; TODO this requires error to be a condition class
        (error error))))

(declaim
 (ftype (function (string transceiver:client)
                  (values message:message &optional))
        find-server-message))
(defun find-server-message (message-name client)
  (let ((messages (messages (transceiver:server-protocol client))))
    (find message-name messages
          :test #'string= :key #'closer-mop:generic-function-name)))

(defmacro update-client-cache (client handshake checkp)
  (declare (symbol client handshake)
           (boolean checkp))
  (let* ((server-hash (gensym))
         (server-protocol-string (gensym))
         (server-protocol `(io:deserialize 'protocol ,server-protocol-string))
         (server-hash-accessor `(transceiver:server-hash ,client))
         (server-protocol-accessor `(transceiver:server-protocol ,client)))
    `(let ((,server-hash
             (schema:object
              (handshake:server-hash ,handshake)))
           (,server-protocol-string
             (schema:object
              (handshake:server-protocol ,handshake))))
       ,@(if checkp
             `((when ,server-hash
                 (setf ,server-hash-accessor ,server-hash))
               (when ,server-protocol-string
                 (setf ,server-protocol-accessor ,server-protocol)))
             `((setf ,server-hash-accessor ,server-hash
                     ,server-protocol-accessor ,server-protocol))))))

(defmacro handshake-match (handshake &body cases)
  (declare (symbol handshake))
  (let ((known-cases
          (map 'list #'intern (schema:symbols (find-class 'handshake:match)))))
    (map nil
         (lambda (case)
           (check-type case cons)
           (let ((position (position (first case) known-cases)))
             (unless position
               (error "Unknown case ~S, expected one of ~S"
                      (first case) known-cases))
             (rplaca case position)))
         cases))
  `(ecase (nth-value 1 (schema:which-one
                        (handshake:match ,handshake)))
     ,@cases))

(declaim
 (ftype (function (protocol
                   transceiver:client
                   string
                   schema:record-object
                   handshake:map<bytes>)
                  (values framing:input-stream &optional))
        perform-handshake))
(defun perform-handshake (protocol client message-name parameters metadata)
  (let* ((request-handshake (initial-handshake protocol client))
         (buffers (framing:frame
                   request-handshake metadata message-name parameters))
         (response-stream (framing:to-input-stream
                           (transceiver:send-and-receive client buffers)))
         (response-handshake (io:deserialize
                              'handshake:response response-stream)))
    (handshake-match response-handshake
      (BOTH)
      (CLIENT
       (update-client-cache client response-handshake nil))
      (NONE
       (update-client-cache client response-handshake nil)
       (setf (elt buffers 0) (framing:buffer
                              (subsequent-handshake protocol client))
             response-stream (framing:to-input-stream
                              (transceiver:send-and-receive client buffers))
             response-handshake (io:deserialize
                                 'handshake:response response-stream))
       (handshake-match response-handshake
         (BOTH))))
    response-stream))

(declaim
 (ftype (function (protocol transceiver:client)
                  (values handshake:request &optional))
        initial-handshake))
(defun initial-handshake (protocol client)
  (make-instance
   'handshake:request
   :client-hash (md5 protocol)
   :server-hash (transceiver:server-hash client)))

(declaim
 (ftype (function (protocol transceiver:client)
                  (values handshake:request &optional))
        subsequent-handshake))
(defun subsequent-handshake (protocol client)
  (make-instance
   'handshake:request
   :client-hash (md5 protocol)
   :client-protocol (io:serialize protocol)
   :server-hash (transceiver:server-hash client)))

(declaim
 (ftype (function (list) (values handshake:map<bytes> &optional))
        parse-metadata))
(defun parse-metadata (lambda-list)
  ;; TODO change lambda-list to allow passing in request/response
  ;; metadata as either a single keyword arg or as a rest plist of
  ;; strings and bytes...symbols should be fine too
  (declare (ignore lambda-list))
  (make-instance 'handshake:map<bytes>))

(declaim
 (ftype (function (schema:record list) (values schema:record-object &optional))
        parameters-record))
(defun parameters-record (request lambda-list)
  (loop
    for field across (schema:fields request)
    for arg in lambda-list

    for name = (schema:name field)
    for keyword = (intern name 'keyword)

    collect keyword into initargs
    collect arg into initargs

    finally
       (return
         (apply #'make-instance request initargs))))

(defmethod make-methods
    ((protocol protocol)
     (client transceiver:stateful-client)
     (messages simple-array))
  (declare ((simple-array message:message (*)) messages))
  (flet ((make-stateful-method (message)
           (make-stateful-method protocol client message)))
    (map '(simple-array standard-method (*)) #'make-stateful-method messages)))

(declaim
 (ftype (function (protocol transceiver:stateful-client message:message)
                  (values standard-method &optional))
        make-stateful-method))
(defun make-stateful-method (client message)
  )

(defmethod make-methods
    ((protocol protocol)
     (server transceiver:server)
     (messages simple-array))
  (declare ((simple-array message:message (*)) messages)))
