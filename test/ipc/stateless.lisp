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
(defpackage #:cl-avro/test/ipc/stateless/server
  (:use #:cl)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/ipc/common
                #:flatten)
  (:export #:service-endpoint
           #:*one-way*
           #:*metadata*))
(in-package #:cl-avro/test/ipc/stateless/server)

(defclass |Greeting| ()
  ((|message| :type avro:string :initarg :message))
  (:metaclass avro:record)
  (:enclosing-namespace "com.acme"))

(avro:define-error |Curse|
    ((|message| :type avro:string :initarg :message)))

(defclass |HelloWorld| ()
  ()
  (:metaclass avro:protocol)
  (:documentation "Protocol Greetings")
  (:namespace "com.acme")
  (:types
   |Greeting|
   |Curse|)
  (:messages
   (:name |hello|
    :documentation "Say hello."
    :request ((:name #:|greeting| :type |Greeting|))
    :response |Greeting|
    :errors (|Curse|))
   (:name one-way
    :request ((:name #:foo :type avro:string))
    :response avro:null
    :one-way t)))

(defclass server (avro:server)
  ((cache
    :initarg :cache
    :type hash-table))
  (:default-initargs
   :cache (make-hash-table :test #'equalp)))

(defmethod avro:client-protocol
    ((server server) client-hash)
  (with-slots (cache) server
    (gethash client-hash cache)))

(defmethod (setf avro:client-protocol)
    (client-protocol (server server) client-hash)
  (with-slots (cache) server
    (setf (gethash client-hash cache) client-protocol)))

(defparameter +server+ (make-instance 'server))

(defparameter +service+
  (make-instance '|HelloWorld| :transceiver +server+))

(defparameter *metadata* nil)

;; TODO maybe have finalize-inheritance assert an applicable method
;; exists for each message
(defmethod |hello| ((greeting |Greeting|) &optional metadata)
  (check-type metadata avro:map<bytes>)
  (setf *metadata* metadata)
  (let ((message (slot-value greeting '|message|)))
    (cond
      ((string= message "foo")
       (error '|Curse| :message "throw foo"))
      ((string= message "bar") (error "meow"))
      ((string= message "baz")
       (values (make-instance '|Greeting| :message "baz with metadata")
               (let ((map (make-instance 'avro:map<bytes>)))
                 (setf (avro:gethash "baz" map)
                       (babel:string-to-octets message :encoding :utf-8))
                 map)))
      (t (make-instance '|Greeting| :message (format nil "Hello ~A!" message))))))

(defparameter *one-way* nil)

(defmethod one-way ((string string) &optional metadata)
  (check-type metadata avro:map<bytes>)
  (setf *one-way* string
        *metadata* metadata))

(declaim
 (ftype (function (cl-avro.internal.ipc.framing:buffers)
                  (values (simple-array (unsigned-byte 8) (*)) &optional))
        service-endpoint))
(defun service-endpoint (request)
  (let* ((request (flatten request))
         (response (avro:receive-from-unconnected-client +service+ request)))
    (flatten response)))

(defpackage #:cl-avro/test/ipc/stateless/client
  (:use #:cl)
  (:local-nicknames
   (#:avro #:cl-avro)
   (#:server #:cl-avro/test/ipc/stateless/server))
  (:export #:|Greeting|
           #:|Curse|
           #:|message|
           #:|hello|
           #:one-way))
(in-package #:cl-avro/test/ipc/stateless/client)

(defclass |Greeting| ()
  ((|message| :type avro:string :initarg :message))
  (:metaclass avro:record)
  (:enclosing-namespace "com.acme"))

(avro:define-error |Curse|
    ((|message| :type avro:string :reader |message|)))

(defclass |HelloWorld| ()
  ()
  (:metaclass avro:protocol)
  (:documentation "Protocol Greetings")
  (:namespace "com.acme")
  (:types
   |Greeting|
   |Curse|)
  (:messages
   (:name |hello|
    :documentation "Say hello."
    :request ((:name #:|greeting| :type |Greeting|))
    :response |Greeting|
    :errors (|Curse|))
   (:name one-way
    :request ((:name #:foo :type avro:string))
    :response avro:null
    :one-way t)))

(defclass client (avro:stateless-client)
  ())

(defmethod avro:send-and-receive
    ((client client) buffers)
  (server:service-endpoint buffers))

(defparameter +client+ (make-instance 'client))

(defparameter +service+
  (make-instance '|HelloWorld| :transceiver +client+))

(defpackage #:cl-avro/test/ipc/stateless
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)
   (#:client #:cl-avro/test/ipc/stateless/client)
   (#:server #:cl-avro/test/ipc/stateless/server)))
(in-package #:cl-avro/test/ipc/stateless)

(test greeting
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foobar")))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (zerop (avro:hash-table-count response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "Hello foobar!" (slot-value response 'client:|message|))))))

(test greeting-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foobar"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (zerop (avro:hash-table-count response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "Hello foobar!" (slot-value response 'client:|message|))))))

(test metadata-response
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "baz"))
        (expected-response-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "baz" expected-response-metadata)
          (babel:string-to-octets "baz" :encoding :utf-8))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (equalp (avro:raw expected-response-metadata)
                  (avro:raw response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "baz with metadata" (slot-value response 'client:|message|))))))

(test metadata-response-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "baz"))
        (request-metadata (make-instance 'avro:map<bytes>))
        (expected-response-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12))
          (avro:gethash "baz" expected-response-metadata)
          (babel:string-to-octets "baz" :encoding :utf-8))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (equalp (avro:raw expected-response-metadata)
                  (avro:raw response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "baz with metadata" (slot-value response 'client:|message|))))))

(test declared-error
  (setf server:*metadata* nil)
  (handler-case
      (let ((request (make-instance 'client:|Greeting| :message "foo")))
        (client:|hello| request))
    (client:|Curse| (curse)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (typep curse 'client:|Curse|))
      (is (string= "throw foo" (client:|message| curse))))))

(test declared-error-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foo"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (handler-case
        (client:|hello| request request-metadata)
      (client:|Curse| (curse)
        (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
        (is (typep curse 'client:|Curse|))
        (is (string= "throw foo" (client:|message| curse)))))))

(test undeclared-error
  (setf server:*metadata* nil)
  (handler-case
      (let ((request (make-instance 'client:|Greeting| :message "bar")))
        (client:|hello| request))
    (avro:undeclared-rpc-error (condition)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (typep condition 'avro:undeclared-rpc-error))
      (is (string= "oh no, an error occurred" (avro:message condition))))))

(test undeclared-error-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "bar"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (handler-case
        (client:|hello| request request-metadata)
      (avro:undeclared-rpc-error (condition)
        (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
        (is (typep condition 'avro:undeclared-rpc-error))
        (is (string= "oh no, an error occurred" (avro:message condition)))))))

(test one-way
  (setf server:*one-way* nil
        server:*metadata* nil)
  (let ((request "foobar"))
    (multiple-value-bind (response response-metadata)
        (client:one-way request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (null response))
      (is (null response-metadata))
      (is (string= request server:*one-way*)))))

(test one-way-metadata
  (setf server:*one-way* nil
        server:*metadata* nil)
  (let ((request "foobar")
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (multiple-value-bind (response response-metadata)
        (client:one-way request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (null response))
      (is (null response-metadata))
      (is (string= request server:*one-way*)))))
