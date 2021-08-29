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
(defpackage #:cl-avro.ipc.protocol.object.transceiver
  (:use #:cl)
  (:local-nicknames
   (#:protocol #:cl-avro.ipc.protocol.class)
   (#:handshake #:cl-avro.ipc.handshake))
  (:export #:client
           #:stateless-client
           #:stateful-client
           #:send-and-receive
           #:send
           #:send-handshake-p
           #:server-hash
           #:server-protocol
           #:server
           #:client-protocol))
(in-package #:cl-avro.ipc.protocol.object.transceiver)

;;; client

;; caching as slots requires each client to talk to only one server
(defclass client ()
  ((server-hash
    :type handshake:md5
    :accessor server-hash)
   (server-protocol
    :type protocol:protocol
    :accessor server-protocol)))

(defclass stateless-client (client)
  ())

(defclass stateful-client (client)
  ())

(defgeneric send-and-receive (client buffers)
  (:documentation
   "Send a message and receive a response.

This is called when sending two-way messages, or when a handshake
needs to be performed."))

(defgeneric send (stateless-client buffers)
  (:documentation
   "Send a one-way message.

This is will be called with stateful-clients when sending a one-way
message over connections that have already performed a handshake."))

(defgeneric send-handshake-p (stateless-client)
  (:documentation
   "Determine if a stateful-client needs to perform a handshake."))

(defgeneric (setf send-handshake-p) (boolean stateless-client)
  (:documentation
   "Called when a stateful-client performs a handshake, or fails to
perform one successfully."))

;;; server

(defclass server ()
  ())

(defgeneric client-protocol (server client-hash)
  (:documentation
   "Return the client protocol with hash CLIENT-HASH."))

(defgeneric (setf client-protocol) (client-protocol server client-hash)
  (:documentation
   "Associate CLIENT-HASH with CLIENT-PROTOCOL."))
