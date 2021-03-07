;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
           #:push
           #:pop

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
