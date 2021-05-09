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

(defpackage #:cl-avro.schema
  (:use #:cl-avro.schema.primitive
        #:cl-avro.schema.complex
        #:cl-avro.schema.logical
        #:cl-avro.schema.io
        #:cl-avro.schema.fingerprint
        #:cl-avro.schema.schema-of)
  (:export #:+primitive->name+

           #:json->schema
           #:schema->json
           #:schema-of

           #:fingerprint
           #:*default-fingerprint-algorithm*
           #:crc-64-avro
           #:fingerprint64

           #:schema
           #:complex-schema
           #:primitive-schema
           #:named-schema
           #:object
           #:which-one
           #:default
           #:raw-buffer

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
           #:fullname->name

           #:array
           #:array-object
           #:items
           #:push
           #:pop

           #:enum
           #:enum-object
           #:symbols

           #:fixed
           #:fixed-object
           #:size

           #:map
           #:map-object
           #:values
           #:raw-hash-table
           #:generic-hash-table-count
           #:generic-hash-table-p
           #:generic-hash-table-size
           #:hashclr
           #:hashmap
           #:hashref
           #:hashrem

           #:union
           #:union-object
           #:schemas
           #:object

           #:record
           #:record-object
           #:fields
           #:name->field

           #:field
           #:type
           #:order
           #:ascending
           #:descending
           #:ignore

           #:logical-schema
           #:underlying

           #:uuid-schema
           #:uuid

           #:date-schema
           #:date
           #:year
           #:month
           #:day

           #:time-millis-schema
           #:time-micros-schema
           #:time-millis
           #:time-micros
           #:hour
           #:minute
           #:second

           #:timestamp-millis-schema
           #:timestamp-micros-schema
           #:timestamp-millis
           #:timestamp-micros

           #:local-timestamp-millis-schema
           #:local-timestamp-micros-schema
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
