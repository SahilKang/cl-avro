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

(defpackage #:cl-avro.schema.complex
  (:use #:cl-avro.schema.complex.base
        #:cl-avro.schema.complex.named
        #:cl-avro.schema.complex.array
        #:cl-avro.schema.complex.enum
        #:cl-avro.schema.complex.fixed
        #:cl-avro.schema.complex.map
        #:cl-avro.schema.complex.union
        #:cl-avro.schema.complex.record)
  (:export #:schema
           #:object
           #:which-one
           #:default
           #:raw-buffer

           #:complex-schema
           #:complex-object
           #:ensure-superclass

           #:named-schema
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
           #:ignore))
