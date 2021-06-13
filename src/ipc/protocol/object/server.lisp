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
(defpackage #:cl-avro.ipc.protocol.object.server
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:message #:cl-avro.ipc.message)
   (#:error #:cl-avro.ipc.error)
   (#:transceiver #:cl-avro.ipc.protocol.object.transceiver)
   (#:handshake #:cl-avro.ipc.handshake)
   (#:framing #:cl-avro.ipc.framing)
   (#:protocol #:cl-avro.ipc.protocol.class))
  (:import-from #:cl-avro.ipc.protocol.object.protocol-object
                #:protocol-object
                #:transceiver)
  (:export #:receive-from-unconnected-client
           #:receive-from-connected-client))
(in-package #:cl-avro.ipc.protocol.object.server)

(declaim
 (ftype (function
         (schema:record
          message:message
          framing:input-stream)
         (values schema:object handshake:map<bytes> schema:boolean &optional))
        call-function))
(defun call-function (client-request server-message request-stream)
  (let* ((parameters
           (io:deserialize
            client-request
            request-stream
            :reader-schema (message:request server-message)))
         (lambda-list
           (map
            'list
            (lambda (field)
              (slot-value parameters (nth-value 1 (schema:name field))))
            (schema:fields (class-of parameters))))
         (errors-union (nth-value 1 (message:errors server-message))))
    (handler-case
        (multiple-value-bind (response response-metadata)
            (apply server-message lambda-list)
          (values
           response
           (or response-metadata (make-instance 'handshake:map<bytes>))
           'schema:false))
      (error:declared-rpc-error (error)
        (values
         ;; TODO this can also signal if error is not part of
         ;; union. In addition to adding another handler-case, I can
         ;; also subclass standard-method and provide a signal-error
         ;; function within its scope, like call-next-method
         (make-instance errors-union :object (error:to-record error))
         (error:metadata error)
         'schema:true))
      (condition ()
        (values
         (make-instance errors-union :object "oh no, an error occurred")
         (make-instance 'handshake:map<bytes>)
         'schema:true)))))

;;; receive-from-unconnected-client

(declaim
 (ftype (function (protocol-object (or (vector (unsigned-byte 8)) stream))
                  (values framing:buffers (or protocol:protocol null) &optional))
        receive-from-unconnected-client))
(defun receive-from-unconnected-client (protocol-object input)
  "Perform a handshake and generate a response.

If the handshake is complete, then the second return value will be the
client-protocol. Otherwise, it will be nil."
  (let* ((request-stream (framing:to-input-stream input))
         (request-handshake (io:deserialize 'handshake:request request-stream))
         ;; TODO pass this in to the message as keyword args then the
         ;; lambda-list will be something like:
         ;; ((arg1 foo) (arg2 bar) &rest metadata &key &allow-other-keys)
         ;; make sure metadata is interned so as not to conflict with
         ;; required args
         (request-metadata
           (io:deserialize 'handshake:map<bytes> request-stream))
         (message-name (io:deserialize 'schema:string request-stream)))
    (declare (ignore request-metadata))
    (multiple-value-bind (response-handshake client-protocol)
        (handshake-response protocol-object request-handshake)
      (if (or (zerop (length message-name))
              (null client-protocol))
          (values
           (framing:frame response-handshake)
           client-protocol)
          (let ((client-request
                  (message:request
                   (find message-name
                         (protocol:messages client-protocol)
                         :test #'string=
                         :key #'closer-mop:generic-function-name)))
                (server-message
                  (find message-name
                        (protocol:messages (class-of protocol-object))
                        :test #'string=
                        :key #'closer-mop:generic-function-name)))
            (multiple-value-bind (response response-metadata errorp)
                (call-function client-request server-message request-stream)
              (values
               (framing:frame
                response-handshake response-metadata errorp response)
               client-protocol)))))))

(declaim
 (ftype (function
         (protocol-object handshake:request)
         (values handshake:response (or protocol:protocol null) &optional))
        handshake-response))
(defun handshake-response (protocol-object request-handshake)
  (let* ((client-hash
           (schema:raw-buffer
            (handshake:client-hash request-handshake)))
         (server-hash
           (schema:raw-buffer
            (handshake:server-hash request-handshake)))
         (client-protocol
           (schema:object
            (handshake:client-protocol request-handshake)))
         (server (transceiver protocol-object))
         (protocol (class-of protocol-object))
         (md5 (handshake:md5 protocol)))
    (if client-protocol
        (let ((client-protocol
                (io:deserialize 'protocol:protocol client-protocol)))
          (setf (transceiver:client-protocol server client-hash)
                client-protocol)
          (if (equalp server-hash (schema:raw-buffer md5))
              (values
               (make-instance
                'handshake:response
                :match
                (make-instance 'handshake:match :enum "BOTH")
                :server-protocol
                (make-instance 'handshake:union<null-string> :object nil)
                :server-hash
                (make-instance 'handshake:union<null-md5> :object nil))
               client-protocol)
              (values
               (make-instance
                'handshake:response
                :match
                (make-instance 'handshake:match :enum "CLIENT")
                :server-protocol
                (make-instance
                 'handshake:union<null-string>
                 :object (io:serialize protocol))
                :server-hash
                (make-instance 'handshake:union<null-md5> :object md5))
               client-protocol)))
        (let ((client-protocol
                (transceiver:client-protocol server client-hash)))
          (if client-protocol
              (if (equalp server-hash (schema:raw-buffer md5))
                  (values
                   (make-instance
                    'handshake:response
                    :match
                    (make-instance 'handshake:match :enum "BOTH")
                    :server-protocol
                    (make-instance 'handshake:union<null-string> :object nil)
                    :server-hash
                    (make-instance 'handshake:union<null-md5> :object nil))
                   client-protocol)
                  (values
                   (make-instance
                    'handshake:response
                    :match
                    (make-instance 'handshake:match :enum "CLIENT")
                    :server-protocol
                    (make-instance
                     'handshake:union<null-string>
                     :object (io:serialize protocol))
                    :server-hash
                    (make-instance 'handshake:union<null-md5> :object md5))
                   client-protocol))
              (if (equalp server-hash (schema:raw-buffer md5))
                  (values
                   (make-instance
                    'handshake:response
                    :match
                    (make-instance 'handshake:match :enum "NONE")
                    :server-protocol
                    (make-instance 'handshake:union<null-string> :object nil)
                    :server-hash
                    (make-instance 'handshake:union<null-md5> :object nil))
                   nil)
                  (values
                   (make-instance
                    'handshake:response
                    :match
                    (make-instance 'handshake:match :enum "NONE")
                    :server-protocol
                    (make-instance
                     'handshake:union<null-string>
                     :object (io:serialize protocol))
                    :server-hash
                    (make-instance 'handshake:union<null-md5> :object md5))
                   nil)))))))

;;; receive-from-connected-client

(declaim
 (ftype (function (protocol-object
                   (or (vector (unsigned-byte 8)) stream)
                   protocol:protocol)
                  (values (or null framing:buffers) &optional))
        receive-from-connected-client))
(defun receive-from-connected-client (protocol-object input client-protocol)
  "Generate a response without performing a handshake.

A return value of nil indicates no response should be sent to the
client."
  (let* ((request-stream (framing:to-input-stream input))
         (request-metadata
           (io:deserialize 'handshake:map<bytes> request-stream))
         (message-name (io:deserialize 'schema:string request-stream)))
    (declare (ignore request-metadata))
    (if (zerop (length message-name))
        ;; should any data be returned, aside from a transport
        ;; response?  I think yes: not every transport is guaranteed
        ;; to have its own notion of an empty response. i.e. I need to
        ;; send /some/ bytes
        (framing:frame)
        (let ((client-request
                (message:request
                 (find message-name
                       (protocol:messages client-protocol)
                       :test #'string=
                       :key #'closer-mop:generic-function-name)))
              (server-message
                (find message-name
                      (protocol:messages (class-of protocol-object))
                      :test #'string=
                      :key #'closer-mop:generic-function-name)))
          (multiple-value-bind (response response-metadata errorp)
              (call-function client-request server-message request-stream)
            (unless (message:one-way server-message)
              (framing:frame response-metadata errorp response)))))))
