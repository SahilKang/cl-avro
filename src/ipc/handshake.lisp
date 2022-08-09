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
(defpackage #:cl-avro.internal.ipc.handshake
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)))
(in-package #:cl-avro.internal.ipc.handshake)

(defclass internal:md5 ()
  ()
  (:metaclass api:fixed)
  (:size 16)
  (:enclosing-namespace "org.apache.avro.ipc"))

(defclass internal:union<null-md5> ()
  ()
  (:metaclass api:union)
  (:schemas api:null internal:md5))

(defclass internal:union<null-string> ()
  ()
  (:metaclass api:union)
  (:schemas api:null api:string))

(defclass api:map<bytes> ()
  ()
  (:metaclass api:map)
  (:values api:bytes))

(defclass internal:union<null-map<bytes>> ()
  ()
  (:metaclass api:union)
  (:schemas api:null api:map<bytes>))

(defclass internal:match ()
  ()
  (:metaclass api:enum)
  (:symbols "BOTH" "CLIENT" "NONE")
  (:name "HandshakeMatch")
  (:enclosing-namespace "org.apache.avro.ipc"))

;;; request

(defclass internal:request ()
  ((|clientHash|
    :initarg :client-hash
    :type internal:md5
    :reader internal:client-hash)
   (|clientProtocol|
    :initarg :client-protocol
    :type internal:union<null-string>
    :reader internal:client-protocol)
   (|serverHash|
    :initarg :server-hash
    :type internal:md5
    :reader internal:server-hash)
   (|meta|
    :initarg :meta
    :type internal:union<null-map<bytes>>
    :reader internal:meta))
  (:metaclass api:record)
  (:name "HandshakeRequest")
  (:namespace "org.apache.avro.ipc")
  (:default-initargs
   :client-protocol (make-instance 'internal:union<null-string> :object nil)
   :meta (make-instance 'internal:union<null-map<bytes>> :object nil)))

;;; response

(defclass internal:response ()
  ((|match|
    :initarg :match
    :type internal:match
    :reader internal:match)
   (|serverProtocol|
    :initarg :server-protocol
    :type internal:union<null-string>
    :reader internal:server-protocol)
   (|serverHash|
    :initarg :server-hash
    :type internal:union<null-md5>
    :reader internal:server-hash)
   (|meta|
    :initarg :meta
    :type internal:union<null-map<bytes>>
    :reader internal:meta))
  (:metaclass api:record)
  (:name "HandshakeResponse")
  (:namespace "org.apache.avro.ipc")
  (:default-initargs
   :server-protocol (make-instance 'internal:union<null-string> :object nil)
   :server-hash (make-instance 'internal:union<null-md5> :object nil)
   :meta (make-instance 'internal:union<null-map<bytes>> :object nil)))
