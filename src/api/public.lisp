;;; Copyright 2022-2024 Google LLC
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

(defpackage #:cl-avro
  (:export #:schema
           #:object
           #:intern
           #:*null-namespace*
           #:schema-of
           #:fingerprint
           #:*default-fingerprint-algorithm*
           #:primitive-schema
           #:primitive-object
           #:boolean
           #:true
           #:false
           #:bytes
           #:double
           #:float
           #:int
           #:long
           #:null
           #:string
           #:complex-schema
           #:complex-object
           #:array
           #:items
           #:array-object
           #:raw
           #:push
           #:pop
           #:map
           #:values
           #:map-object
           #:hash-table-count
           #:hash-table-size
           #:clrhash
           #:maphash
           #:gethash
           #:remhash
           #:union
           #:union-object
           #:schemas
           #:which-one
           #:fixed
           #:fixed-object
           #:size
           #:enum
           #:enum-object
           #:symbols
           #:default
           #:record
           #:*add-accessors-and-initargs-p*
           #:record-object
           #:fields
           #:field
           #:name
           #:aliases
           #:type
           #:order
           #:ascending
           #:descending
           #:ignore
           #:logical-schema
           #:logical-object
           #:uuid
           #:date
           #:year
           #:month
           #:day
           #:time-millis
           #:time-micros
           #:hour
           #:minute
           #:second
           #:millisecond
           #:microsecond
           #:timestamp-millis
           #:timestamp-micros
           #:local-timestamp-millis
           #:local-timestamp-micros
           #:decimal
           #:decimal-object
           #:precision
           #:unscaled
           #:scale
           #:duration
           #:duration-object
           #:months
           #:days
           #:milliseconds
           #:name-return-type
           #:namespace
           #:namespace-return-type
           #:fullname
           #:rpc-error
           #:metadata
           #:undeclared-rpc-error
           #:message
           #:declared-rpc-error
           #:define-error
           #:request
           #:response
           #:errors
           #:one-way
           #:protocol
           #:types
           #:messages
           #:protocol-object
           #:transceiver
           #:stateless-client
           #:stateful-client
           #:send
           #:send-and-receive
           #:sent-handshake-p
           #:server
           #:client-protocol
           #:receive-from-unconnected-client
           #:receive-from-connected-client
           #:serialize
           #:deserialize
           #:serialized-size
           #:magic
           #:meta
           #:sync
           #:file-header
           #:codec
           #:file-block
           #:count
           #:*decompress-deflate*
           #:*decompress-bzip2*
           #:*decompress-snappy*
           #:*decompress-xz*
           #:*decompress-zstandard*
           #:*compress-deflate*
           #:*compress-bzip2*
           #:*compress-snappy*
           #:*compress-xz*
           #:*compress-zstandard*
           #:file-reader
           #:skip-block
           #:read-block
           #:file-writer
           #:write-block
           #:compare
           #:coerce
           #:map<bytes>))

(in-package #:cl-avro)

(cl:defgeneric coerce (object schema)
  (:documentation
   "Use Avro Schema Resolution to coerce OBJECT into SCHEMA.

OBJECT may be recursively mutated."))

(cl:defgeneric compare (schema left right cl:&key cl:&allow-other-keys)
  (:documentation
   "Return 0, -1, or 1 if LEFT is equal to, less than, or greater than RIGHT.

LEFT and RIGHT should be avro serialized data.

LEFT and RIGHT may not necessarily be fully consumed."))

(cl:defgeneric codec (instance))

(cl:defgeneric schema (instance))

(cl:defgeneric intern (instance cl:&key null-namespace))

(cl:defgeneric serialized-size (object))

(cl:defgeneric deserialize (schema input cl:&key cl:&allow-other-keys)
  (:documentation
   "Deserialize INPUT according to SCHEMA.

If SCHEMA is eql to the symbol SCHEMA or PROTOCOL, then INPUT will be
interpreted as its json representation. Otherwise, INPUT will be interpreted as
the octets of an OBJECT adhering to SCHEMA. SCHEMA may also be eql to
FINGERPRINT, indicating that INPUT is in single object encoding form. In this
latter case, only the fingerprint from INPUT is deserialized.

INPUT may be a stream or vector. If INPUT is a vector, then :START indicates
where to start deserializing from."))

(cl:defgeneric serialize (object cl:&key cl:&allow-other-keys)
  (:documentation
   "Serialize OBJECT, which may be an avro SCHEMA, PROTOCOL, or OBJECT.

If OBJECT is an avro SCHEMA or PROTOCOL, then the json representation of OBJECT
is serialized. Otherwise, if OBJECT is an avro OBJECT, then it's serialized
according to its avro SCHEMA.

:INTO can be a vector or stream, defaulting to a vector if not provided. :INTO
should accept characters for avro SCHEMAs or PROTOCOLs, and octets for avro
OBJECTs. If :INTO is a vector, then :START specifies where to start storing the
output.

For avro SCHEMAs or PROTOCOLs, :CANONICAL-FORM-P will determine if the json
output is in avro canonical form.

For avro OBJECTs, :SINGLE-OBJECT-ENCODING-P will determine if the octet output
adheres to avro single object encoding."))

(cl:defgeneric one-way (message))

(cl:defgeneric errors (message))

(cl:defgeneric namespace (named-class))

(cl:defgeneric name (named-class))

(cl:defgeneric milliseconds (object))

(cl:defgeneric days (object))

(cl:defgeneric months (object))

(cl:defgeneric scale (schema))

(cl:defgeneric microsecond (object cl:&key cl:&allow-other-keys))

(cl:defgeneric millisecond (object cl:&key cl:&allow-other-keys))

(cl:defgeneric second (object cl:&key cl:&allow-other-keys)
  (:documentation "Return (values second remainder)."))

(cl:defgeneric minute (object cl:&key cl:&allow-other-keys))

(cl:defgeneric hour (object cl:&key cl:&allow-other-keys))

(cl:defgeneric day (object cl:&key cl:&allow-other-keys))

(cl:defgeneric month (object cl:&key cl:&allow-other-keys))

(cl:defgeneric year (object cl:&key cl:&allow-other-keys))

(cl:defgeneric schema-of (object))

(cl:defgeneric push (element array))

(cl:defgeneric pop (array))

(cl:defgeneric hash-table-count (map))

(cl:defgeneric hash-table-size (map))

(cl:defgeneric clrhash (map))

(cl:defgeneric maphash (function map))

(cl:defgeneric gethash (key map cl:&optional default))

(cl:defgeneric (cl:setf gethash) (value key map))

(cl:defgeneric remhash (key map))

(cl:defgeneric which-one (object))

(cl:defgeneric default (object))

(cl:defgeneric fields (object))

(cl:defgeneric order (object))

(cl:defgeneric type (object))

(cl:in-package #:cl-user)
