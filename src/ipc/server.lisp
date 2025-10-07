;;; Copyright 2021-2022, 2024 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.ipc.server
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:framing #:cl-avro.internal.ipc.framing))
  (:import-from #:cl-avro.internal.type
                #:vector<uint8>))
(in-package #:cl-avro.internal.ipc.server)

;;; server

(defclass api:server ()
  ()
  (:documentation
   "Base class of avro ipc servers."))

(defgeneric api:client-protocol (server client-hash)
  (:documentation
   "Return the client protocol with hash CLIENT-HASH."))

(defgeneric (setf api:client-protocol) (client-protocol server client-hash)
  (:documentation
   "Associate CLIENT-HASH with CLIENT-PROTOCOL."))

;;; call-function

(declaim
 (ftype (function (api:record api:map<bytes> api:message framing:input-stream)
                  (values api:object api:map<bytes> api:boolean &optional))
        call-function))
(defun call-function
    (client-request request-metadata server-message request-stream)
  (let* ((parameters (api:coerce
                      (api:deserialize client-request request-stream)
                      (api:request server-message)))
         (lambda-list (map
                       'list
                       (lambda (field)
                         (slot-value parameters
                                     (nth-value 1 (api:name field))))
                       (api:fields (class-of parameters))))
         (errors-union (nth-value 1 (api:errors server-message))))
    (handler-case
        (multiple-value-bind (response response-metadata)
            (apply server-message (nconc lambda-list (list request-metadata)))
          (values response
                  (or response-metadata (make-instance 'api:map<bytes>))
                  'api:false))
      (api:declared-rpc-error (error)
        ;; TODO this can also signal if error is not part of union. In
        ;; addition to adding another handler-case, I can also
        ;; subclass standard-method and provide a signal-error
        ;; function within its scope, like call-next-method
        (values (make-instance errors-union :object (internal:to-record error))
                (api:metadata error)
                'api:true))
      (condition ()
        (values (make-instance errors-union :object "oh no, an error occurred")
                (make-instance 'api:map<bytes>)
                'api:true)))))

;;; receive-from-unconnected-client

(declaim
 (ftype (function (api:protocol-object (or vector<uint8> stream))
                  (values framing:buffers (or null api:protocol) &optional))
        api:receive-from-unconnected-client))
(defun api:receive-from-unconnected-client (protocol-object input)
  "Perform a handshake and generate a response.

If the handshake is complete, then the second return value will be the
client-protocol. Otherwise, it will be nil."
  (let* ((request-stream (framing:to-input-stream input))
         (request-handshake (api:deserialize 'internal:request request-stream))
         (request-metadata (api:deserialize 'api:map<bytes> request-stream))
         (message-name (api:deserialize 'api:string request-stream)))
    (multiple-value-bind (response-handshake client-protocol)
        (handshake-response protocol-object request-handshake)
      (if (or (zerop (length message-name))
              (null client-protocol))
          (values (framing:frame response-handshake)
                  client-protocol)
          (let ((client-request
                  (api:request
                   (find message-name
                         (api:messages client-protocol)
                         :test #'string=
                         :key #'closer-mop:generic-function-name)))
                (server-message
                  (find message-name
                        (api:messages (class-of protocol-object))
                        :test #'string=
                        :key #'closer-mop:generic-function-name)))
            (multiple-value-bind (response response-metadata errorp)
                (call-function
                 client-request request-metadata server-message request-stream)
              (values (framing:frame
                       response-handshake response-metadata errorp response)
                      client-protocol)))))))

(declaim
 (ftype (function (api:protocol-object internal:request)
                  (values internal:response (or null api:protocol) &optional))
        handshake-response))
(defun handshake-response (protocol-object request-handshake)
  (let* ((client-hash (api:raw (internal:client-hash request-handshake)))
         (server-hash (api:raw (internal:server-hash request-handshake)))
         (client-protocol (api:object
                           (internal:client-protocol request-handshake)))
         (server (api:transceiver protocol-object))
         (protocol (class-of protocol-object))
         (md5 (internal:md5 protocol)))
    (if client-protocol
        (let ((client-protocol (api:deserialize
                                'api:protocol client-protocol)))
          (setf (api:client-protocol server client-hash) client-protocol)
          (if (equalp server-hash (api:raw md5))
              (values (make-instance
                       'internal:response
                       :match (make-instance 'internal:match :enum "BOTH")
                       :server-protocol (make-instance
                                         'internal:union<null-string>
                                         :object nil)
                       :server-hash (make-instance
                                     'internal:union<null-md5>
                                     :object nil))
                      client-protocol)
              (values
               (make-instance
                'internal:response
                :match (make-instance 'internal:match :enum "CLIENT")
                :server-protocol (make-instance
                                  'internal:union<null-string>
                                  :object (api:serialize protocol))
                :server-hash (make-instance
                              'internal:union<null-md5>
                              :object md5))
               client-protocol)))
        (let ((client-protocol (api:client-protocol server client-hash)))
          (if client-protocol
              (if (equalp server-hash (api:raw md5))
                  (values (make-instance
                           'internal:response
                           :match (make-instance 'internal:match :enum "BOTH")
                           :server-protocol (make-instance
                                             'internal:union<null-string>
                                             :object nil)
                           :server-hash (make-instance
                                         'internal:union<null-md5>
                                         :object nil))
                          client-protocol)
                  (values (make-instance
                           'internal:response
                           :match (make-instance 'internal:match :enum "CLIENT")
                           :server-protocol (make-instance
                                             'internal:union<null-string>
                                             :object (api:serialize protocol))
                           :server-hash (make-instance
                                         'internal:union<null-md5>
                                         :object md5))
                          client-protocol))
              (if (equalp server-hash (api:raw md5))
                  (values (make-instance
                           'internal:response
                           :match (make-instance 'internal:match :enum "NONE")
                           :server-protocol (make-instance
                                             'internal:union<null-string>
                                             :object nil)
                           :server-hash (make-instance
                                         'internal:union<null-md5>
                                         :object nil))
                          nil)
                  (values (make-instance
                           'internal:response
                           :match (make-instance 'internal:match :enum "NONE")
                           :server-protocol (make-instance
                                             'internal:union<null-string>
                                             :object (api:serialize protocol))
                           :server-hash (make-instance
                                         'internal:union<null-md5>
                                         :object md5))
                          nil)))))))

;;; receive-from-connected-client

(declaim
 (ftype (function (api:protocol-object (or vector<uint8> stream) api:protocol)
                  (values (or null framing:buffers) &optional))
        api:receive-from-connected-client))
(defun api:receive-from-connected-client
    (protocol-object input client-protocol)
  "Generate a response without performing a handshake.

A return value of nil indicates no response should be sent to the
client."
  (let* ((request-stream (framing:to-input-stream input))
         (request-metadata (api:deserialize 'api:map<bytes> request-stream))
         (message-name (api:deserialize 'api:string request-stream)))
    (if (zerop (length message-name))
        (framing:frame)
        (let ((client-request (api:request
                               (find message-name
                                     (api:messages client-protocol)
                                     :test #'string=
                                     :key #'closer-mop:generic-function-name)))
              (server-message (find message-name
                                    (api:messages (class-of protocol-object))
                                    :test #'string=
                                    :key #'closer-mop:generic-function-name)))
          (multiple-value-bind (response response-metadata errorp)
              (call-function
               client-request request-metadata server-message request-stream)
            (unless (api:one-way server-message)
              (framing:frame response-metadata errorp response)))))))
