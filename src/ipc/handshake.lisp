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
(defpackage #:cl-avro.ipc.handshake
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:export #:request
           #:response
           #:MD5
           #:union<null-string>
           #:union<null-MD5>
           #:map<bytes>
           #:match
           #:client-hash
           #:client-protocol
           #:server-hash
           #:server-protocol))
(in-package #:cl-avro.ipc.handshake)

;; TODO maybe create and switch into org.apache.avro.ipc package
;; do this for header, etc. then too

(defclass MD5 ()
  ()
  (:metaclass schema:fixed)
  (:enclosing-namespace "org.apache.avro.ipc")
  (:size 16))

(defclass union<null-MD5> ()
  ()
  (:metaclass schema:union)
  (:schemas schema:null MD5))

(defclass union<null-string> ()
  ()
  (:metaclass schema:union)
  (:schemas schema:null schema:string))

(defclass map<bytes> ()
  ()
  (:metaclass schema:map)
  (:values schema:bytes))

(defclass union<null-map<bytes>> ()
  ()
  (:metaclass schema:union)
  (:schemas schema:null map<bytes>))

(defclass match ()
  ()
  (:metaclass schema:enum)
  (:symbols "BOTH" "CLIENT" "NONE")
  (:name "HandshakeMatch")
  (:enclosing-namespace "org.apache.avro.ipc"))

;;; request

(defclass request ()
  ((|clientHash|
    :type MD5
    :initarg :client-hash
    :reader client-hash)
   (|clientProtocol|
    :type union<null-string>
    :initarg :client-protocol
    :reader client-protocol)
   (|serverHash|
    :type MD5
    :initarg :server-hash
    :reader server-hash)
   (|meta|
    :type union<null-map<bytes>>
    :initarg :meta
    :reader meta))
  (:metaclass schema:record)
  (:name "HandshakeRequest")
  (:namespace "org.apache.avro.ipc"))

;;; response

(defclass response ()
  ((|match|
    :type match
    :initarg :match
    :reader match)
   (|serverProtocol|
    :type union<null-string>
    :initarg :server-protocol
    :reader server-protocol)
   (|serverHash|
    :type union<null-MD5>
    :initarg :server-hash
    :reader server-hash)
   (|meta|
    :type union<null-map<bytes>>
    :initarg :meta
    :reader meta))
  (:metaclass schema:record)
  (:name "HandshakeResponse")
  (:namespace "org.apache.avro.ipc"))
