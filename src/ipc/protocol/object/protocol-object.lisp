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
(defpackage #:cl-avro.ipc.protocol.object.protocol-object
  (:use #:cl)
  (:local-nicknames
   (#:transceiver #:cl-avro.ipc.protocol.object.transceiver)
   (#:protocol #:cl-avro.ipc.protocol.class)
   (#:client #:cl-avro.ipc.protocol.object.client))
  (:import-from #:cl-avro.ipc.protocol.class
                #:protocol-object)
  (:export #:protocol-object
           #:transceiver))
(in-package #:cl-avro.ipc.protocol.object.protocol-object)

(defclass protocol-object ()
  ((transceiver
    :initarg :transceiver
    :reader transceiver
    :type (or transceiver:client transceiver:server)
    :documentation "Protocol transceiver."))
  (:default-initargs
   :transceiver (error "Must supply TRANSCEIVER")))

(defmethod initialize-instance :after
    ((instance protocol-object) &key)
  (with-slots (transceiver) instance
    (when (typep transceiver 'transceiver:client)
      (let* ((protocol (class-of instance))
             (messages (protocol:messages protocol)))
        (client:add-methods protocol transceiver messages)
        (setf (transceiver:server-hash transceiver) (protocol:md5 protocol)
              (transceiver:server-protocol transceiver) protocol)))))
