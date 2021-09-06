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
(defpackage #:cl-avro.ipc.protocol.object.client.common
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:resolution #:cl-avro.resolution)
   (#:message #:cl-avro.ipc.message)
   (#:error #:cl-avro.ipc.error)
   (#:transceiver #:cl-avro.ipc.protocol.object.transceiver)
   (#:handshake #:cl-avro.ipc.handshake)
   (#:framing #:cl-avro.ipc.framing)
   (#:protocol #:cl-avro.ipc.protocol.class))
  (:export #:add-methods
           #:parse-metadata
           #:parse-parameters
           #:find-server-message
           #:process-response
           #:perform-handshake))
(in-package #:cl-avro.ipc.protocol.object.client.common)

(defgeneric add-methods (protocol transceiver messages)
  (:method (protocol transceiver (messages null))
    (values))

  (:documentation
   "Add client methods to MESSAGES."))

;;; parse-metadata

(declaim
 (ftype (function (list) (values handshake:map<bytes> &optional))
        parse-metadata))
(defun parse-metadata (lambda-list)
  ;; TODO change lambda-list to allow passing in request/response
  ;; metadata as either a single keyword arg or as a rest plist of
  ;; strings and bytes...symbols should be fine too
  (declare (ignore lambda-list))
  (make-instance 'handshake:map<bytes>))

;;; parse-parameters

(declaim
 (ftype (function (schema:record list) (values schema:record-object &optional))
        parse-parameters))
(defun parse-parameters (request lambda-list)
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

;;; find-server-message

(declaim
 (ftype (function (string transceiver:client)
                  (values message:message &optional))
        find-server-message))
(defun find-server-message (message-name client)
  (let ((messages (protocol:messages (transceiver:server-protocol client))))
    (find message-name messages
          :test #'string= :key #'closer-mop:generic-function-name)))

;;; process-response

(declaim
 (ftype (function (framing:input-stream
                   message:message
                   schema:schema
                   (or null (simple-array class (*)))
                   schema:union)
                  (values schema:object handshake:map<bytes> &optional))
        process-response))
(defun process-response
    (response-stream server-message response-schema conditions errors-union)
  (let ((metadata (io:deserialize 'handshake:map<bytes> response-stream))
        (errorp (eq 'schema:true
                    (io:deserialize 'schema:boolean response-stream))))
    (if errorp
        (error
         (make-error
          conditions
          (resolution:coerce
           (io:deserialize
            (nth-value 1 (message:errors server-message)) response-stream)
           errors-union)
          metadata))
        (values
         (resolution:coerce
          (io:deserialize (message:response server-message) response-stream)
          response-schema)
         metadata))))

(declaim
 (ftype (function ((or null (simple-array class (*)))
                   schema:union-object
                   handshake:map<bytes>)
                  (values error:rpc-error &optional))
        make-error))
(defun make-error (conditions error metadata)
  (let ((position (nth-value 1 (schema:which-one error)))
        (object (schema:object error)))
    (if (zerop position)                ; string is the first schema
        (make-condition
         'error:undeclared-rpc-error :message object :metadata metadata)
        (let ((condition-name (class-name (elt conditions (1- position)))))
          (error:make-declared-rpc-error condition-name object metadata)))))

;;; perform-handshake

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

(defmacro update-client-cache (client handshake checkp)
  (declare (symbol client handshake)
           (boolean checkp))
  (let* ((server-hash (gensym))
         (server-protocol-string (gensym))
         (server-protocol `(io:deserialize 'protocol:protocol ,server-protocol-string))
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

(declaim
 (ftype (function (protocol:protocol
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
       (update-client-cache client response-handshake t)
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
 (ftype (function (protocol:protocol transceiver:client)
                  (values handshake:request &optional))
        initial-handshake))
(defun initial-handshake (protocol client)
  (make-instance
   'handshake:request
   :client-hash (protocol:md5 protocol)
   :server-hash (transceiver:server-hash client)))

(declaim
 (ftype (function (protocol:protocol transceiver:client)
                  (values handshake:request &optional))
        subsequent-handshake))
(defun subsequent-handshake (protocol client)
  (make-instance
   'handshake:request
   :client-hash (protocol:md5 protocol)
   :client-protocol (make-instance 'handshake:union<null-string>
                                   :object (io:serialize protocol))
   :server-hash (transceiver:server-hash client)))
