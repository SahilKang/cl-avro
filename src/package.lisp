;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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
  (:nicknames #:avro)
  (:use #:cl-avro.schema
        #:cl-avro.io
        #:cl-avro.single-object-encoding
        #:cl-avro.object-container-file)
  (:export #:schema
           #:object
           #:schema-of

           #:fingerprint
           #:fingerprint64
           #:crc-64-avro
           #:*default-fingerprint-algorithm*

           #:serialize
           #:deserialize
           #:compare

           #:single-object
           #:single-object-p
           #:write-single-object
           #:single-object->fingerprint
           #:deserialize-single-object

           #:header
           #:magic
           #:meta
           #:sync
           #:schema
           #:codec
           #:null
           #:deflate
           #:snappy
           #:bzip2
           #:xz
           #:zstandard

           #:file-block
           #:count
           #:bytes

           #:file-input-stream
           #:skip-block
           #:read-block

           #:file-output-stream
           #:write-block

           #:null
           #:boolean #:true #:false
           #:int
           #:long
           #:float
           #:double
           #:bytes
           #:string

           #:name
           #:namespace
           #:fullname
           #:aliases

           #:array
           #:array-object
           #:items
           #:objects

           #:enum
           #:enum-object
           #:symbols
           #:default
           #:which-one

           #:fixed
           #:fixed-object
           #:size
           #:bytes

           #:map
           #:map-object
           #:values

           #:union
           #:union-object
           #:schemas
           #:object
           #:which-one

           #:record
           #:record-object
           #:fields

           #:field
           #:type
           #:order
           #:ascending
           #:descending
           #:ignore

           #:underlying

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

           #:timestamp-millis
           #:timestamp-micros

           #:local-timestamp-millis
           #:local-timestamp-micros

           #:decimal
           #:decimal-object
           #:unscaled
           #:scale
           #:precision

           #:duration
           #:duration-object
           #:months
           #:days
           #:milliseconds))

#+nil
(defpackage #:cl-avro
  (:nicknames #:avro)
  (:use #:cl)
  (:export

   #:serialize
   #:deserialize

   #:json->schema
   #:schema->json

   #:compare

   #:fingerprint
   #:avro-64bit-crc
   #:*default-fingerprint-algorithm*

   ;;; avro object container files

   #:file-input-stream
   #:file-output-stream
   #:schema
   #:codec
   #:metadata
   #:sync
   #:read-block
   #:write-block
   #:skip-block

   ;;; avro single object encoding

   #:single-object-p
   #:write-single-object
   #:read-single-object

   ;;; primitive schemas

   #:null-schema
   #:boolean-schema
   #:int-schema
   #:long-schema
   #:float-schema
   #:double-schema
   #:bytes-schema
   #:string-schema

   ;;; complex schemas

   ;; fixed schema

   #:fixed-schema
   #:make-fixed-schema
   #:fixed-schema-p

   #:fixed-schema-name
   #:fixed-schema-namespace
   #:fixed-schema-aliases
   #:fixed-schema-size

   ;; union schema
   
   #:union-schema
   #:make-union-schema
   #:union-schema-p

   #:union-schema-schemas

   #:tagged-union
   #:make-tagged-union
   #:tagged-union-p

   #:tagged-union-value
   #:tagged-union-schema

   ;; array schema

   #:array-schema
   #:make-array-schema
   #:array-schema-p

   #:array-schema-items

   ;; map schema

   #:map-schema
   #:make-map-schema
   #:map-schema-p

   #:map-schema-values

   ;; enum schema

   #:enum-schema
   #:make-enum-schema
   #:enum-schema-p

   #:enum-schema-name
   #:enum-schema-namespace
   #:enum-schema-aliases
   #:enum-schema-doc
   #:enum-schema-symbols
   #:enum-schema-default

   ;; record schema

   #:record-schema
   #:make-record-schema
   #:record-schema-p

   #:record-schema-name
   #:record-schema-namespace
   #:record-schema-aliases
   #:record-schema-doc
   #:record-schema-fields

   #:field-schema
   #:make-field-schema
   #:field-schema-p

   #:field-schema-name
   #:field-schema-aliases
   #:field-schema-doc
   #:field-schema-type
   #:field-schema-order
   #:field-schema-default

   ;;; logical schemas

   #:uuid-schema
   #:date-schema
   #:time-millis-schema
   #:time-micros-schema
   #:timestamp-millis-schema
   #:timestamp-micros-schema
   #:local-timestamp-millis-schema
   #:local-timestamp-micros-schema

   ;; decimal schema

   #:decimal-schema
   #:make-decimal-schema
   #:decimal-schema-p

   #:decimal-schema-underlying-schema
   #:decimal-schema-precision
   #:decimal-schema-scale

   ;; duration schema

   #:duration-schema
   #:make-duration-schema
   #:duration-schema-p

   #:duration-schema-underlying-schema))
