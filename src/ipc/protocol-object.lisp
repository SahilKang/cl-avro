;;; Copyright 2021, 2024 Google LLC
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
(defpackage #:cl-avro.internal.ipc.protocol-object
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)))
(in-package #:cl-avro.internal.ipc.protocol-object)

(defclass api:protocol-object ()
  ((transceiver
    :initarg :transceiver
    :reader api:transceiver
    :type (or internal:client api:server)
    :documentation "Protocol transceiver."))
  (:default-initargs
   :transceiver (error "Must supply TRANSCEIVER"))
  (:documentation
   "Base class of avro protocols."))

(defmethod initialize-instance :after
    ((instance api:protocol-object) &key)
  (with-slots (transceiver) instance
    (when (typep transceiver 'internal:client)
      (let* ((protocol (class-of instance))
             (messages (api:messages protocol)))
        (internal:add-methods protocol transceiver messages)
        (setf (internal:server-hash transceiver) (internal:md5 protocol)
              (internal:server-protocol transceiver) protocol)))))
