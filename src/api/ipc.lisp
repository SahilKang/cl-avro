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

;;; handshake

(in-package #:cl-avro.internal)

(export '(md5 union<null-md5> union<null-string> map<bytes>
          union<null-map<bytes>> match))

(export '(request client-hash client-protocol server-hash meta))

(export '(response match server-protocol server-hash meta))

;;; error

(in-package #:cl-avro)

(export '(rpc-error metadata undeclared-rpc-error message declared-rpc-error
          define-error))

(in-package #:cl-avro.internal)

(export '(make-declared-rpc-error to-record))

(export 'schema)
(defgeneric schema (error))

;;; message

(in-package #:cl-avro)

(export '(message request response))

(export 'errors)
(defgeneric errors (message))

(export 'one-way)
(defgeneric one-way (message))

;;; protocol

(in-package #:cl-avro)

(export '(protocol types messages))

(export '(protocol-object transceiver))

;;; client

(in-package #:cl-avro.internal)

(export '(client add-methods server-hash server-protocol))

(in-package #:cl-avro)

(export '(stateless-client stateful-client))

(export '(send send-and-receive sent-handshake-p))

;;; server

(in-package #:cl-avro)

(export '(server client-protocol
          receive-from-unconnected-client receive-from-connected-client))
