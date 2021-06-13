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
(defpackage #:cl-avro.ipc.transceiver
  (:use #:cl)
  (:export #:client
           #:stateless-client
           #:stateful-client
           #:send-and-receive
           #:send
           #:send-handshake-p
           #:server
           #:receive
           #:server-hash
           #:server-protocol))
(in-package #:cl-avro.ipc.transceiver)

;;; client

;; caching as slots requires each client to talk to only one server
(defclass client ()
  ((server-hash
    :type simple-string
    :accessor server-hash)
   (server-protocol
    :type #+nil protocol t ; TODO need to separate protocol and
                           ; protocol-object
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

;; TODO will need to allow user to configuer caching policy:
;; client-hasn->client-protocol
(defclass server ()
  ())

;; hmm, this should actually just be a function that does the
;; dispatching and returns (or null buffers)
(defgeneric receive (server buffers)
  (:documentation
   "Receive buffers and generate a response."))
