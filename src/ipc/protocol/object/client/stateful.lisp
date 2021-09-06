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
(defpackage #:cl-avro.ipc.protocol.object.client.stateful
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:message #:cl-avro.ipc.message)
   (#:transceiver #:cl-avro.ipc.protocol.object.transceiver)
   (#:protocol #:cl-avro.ipc.protocol.class)
   (#:framing #:cl-avro.ipc.framing))
  (:import-from #:cl-avro.ipc.protocol.object.client.common
                #:add-methods
                #:parse-metadata
                #:parse-parameters
                #:find-server-message
                #:process-response
                #:perform-handshake)
  (:export #:add-methods))
(in-package #:cl-avro.ipc.protocol.object.client.stateful)

(defmethod add-methods
    ((protocol protocol:protocol)
     (client transceiver:stateful-client)
     (messages simple-array))
  (declare ((simple-array message:message (*)) messages))
  (flet ((add-stateful-method (message)
           (add-stateful-method protocol client message)))
    (map nil #'add-stateful-method messages))
  (values))

(declaim
 (ftype
  (function (protocol:protocol transceiver:stateful-client message:message)
            (values &optional))
  add-stateful-method))
(defun add-stateful-method (protocol client message)
  (let ((message-name (string (closer-mop:generic-function-name message)))
        (lambda-list (closer-mop:generic-function-lambda-list message))
        (request-schema (message:request message))
        (response-schema (message:response message)))
    (multiple-value-bind (conditions errors-union)
        (message:errors message)
      (let* ((body
               `(lambda ,lambda-list
                  (let* ((metadata (parse-metadata (list ,@lambda-list)))
                         (parameters (parse-parameters ,request-schema (list ,@lambda-list)))
                         ,@(unless (message:one-way message)
                             `((response-stream
                                (if (not (transceiver:send-handshake-p ,client))
                                    (framing:to-input-stream
                                     (transceiver:send-and-receive
                                      ,client (framing:frame metadata ,message-name parameters)))
                                    (prog1 (perform-handshake
                                            ,protocol ,client ,message-name parameters metadata)
                                      (setf (transceiver:send-handshake-p ,client) nil)))))))
                    ,@(if (message:one-way message)
                          `((if (not (transceiver:send-handshake-p ,client))
                                (transceiver:send
                                 ,client (framing:frame metadata ,message-name parameters))
                                (progn
                                  (perform-handshake
                                   ,protocol ,client ,message-name parameters metadata)
                                  (setf (transceiver:send-handshake-p ,client) nil)))
                            nil)
                          `((process-response
                             response-stream
                             (find-server-message ,message-name ,client)
                             ,response-schema
                             ,conditions
                             ,errors-union))))))
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
                            (find-class (schema:primitive->class schema))
                            schema)))
                    (schema:fields request-schema)))
             (method
               (make-instance
                'standard-method
                :lambda-list lambda-list
                :specializers specializers
                :function (compile nil method-lambda)
                :documentation documentation)))
        (add-method message method))))
  (values))
