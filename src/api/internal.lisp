;;; Copyright 2022, 2024 Google LLC
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

(defpackage #:cl-avro.internal
  (:use)
  (:export #:fixed-size
           #:serialize-field-default
           #:deserialize-field-default
           #:skip
           #:underlying
           #:readers
           #:writers
           #:duplicates
           #:md5
           #:union<null-md5>
           #:union<null-string>
           #:union<null-map<bytes>>
           #:match
           #:request
           #:client-hash
           #:client-protocol
           #:server-hash
           #:meta
           #:response
           #:server-protocol
           #:make-declared-rpc-error
           #:to-record
           #:schema
           #:client
           #:add-methods
           #:serialize
           #:crc-64-avro-little-endian
           #:read-jso
           #:with-initargs
           #:write-jso
           #:write-json
           #:logical-name
           #:downcase-symbol))

(in-package #:cl-avro.internal)

(cl:defgeneric logical-name (schema))

(cl:defgeneric write-jso (schema seen canonical-form-p))

(cl:defgeneric write-json (jso-object into cl:&key cl:&allow-other-keys))

(cl:defgeneric crc-64-avro-little-endian (schema))

(cl:defgeneric serialize (object into cl:&key cl:&allow-other-keys))

(cl:defgeneric schema (error))

(cl:defgeneric writers (effective-field))

(cl:defgeneric readers (effective-field))

(cl:defgeneric underlying (schema))

(cl:defgeneric skip (schema input cl:&optional start))

(cl:defgeneric fixed-size (schema))

(cl:defgeneric serialize-field-default (default))

(cl:defgeneric deserialize-field-default (schema default))

(cl:in-package #:cl-user)
