;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(in-package #:asdf-user)

(defsystem #:cl-avro
  :description "Implementation of the Apache Avro data serialization system."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :build-pathname "cl-avro"
  :pathname "src"
  :in-order-to ((test-op (test-op #:cl-avro/test)))
  :depends-on (#:alexandria
               #:babel
               #:chipz
               #:closer-mop
               #:ieee-floats
               #:flexi-streams
               #:local-time
               #:local-time-duration
               #:md5
               #:salza2
               #:st-json
               #:time-interval
               #:trivial-extensible-sequences)
  :components
  ((:file "type")
   (:file "mop")
   (:file "ascii")
   (:file "little-endian" :depends-on ("type"))
   (:file "crc-64-avro" :depends-on ("type"))
   (:module "api"
    :components ((:file "package")
                 (:file "io" :depends-on ("package"))
                 (:file "schema" :depends-on ("package"))
                 (:file "name" :depends-on ("package"))
                 (:file "coerce" :depends-on ("package"))
                 (:file "compare" :depends-on ("package"))
                 (:file "file" :depends-on ("package"))
                 (:file "ipc" :depends-on ("package"))))
   (:module "recursive-descent"
    :depends-on ("api" "mop" "type")
    :components ((:file "pattern")
                 (:file "jso" :depends-on ("pattern"))))
   (:module "primitive"
    :depends-on ("type" "little-endian" "crc-64-avro" "api" "recursive-descent")
    :components ((:file "defprimitive")
                 (:file "compare")
                 (:file "ieee-754" :depends-on ("compare"))
                 (:file "zigzag" :depends-on ("compare"))
                 (:file "null" :depends-on ("defprimitive"))
                 (:file "boolean" :depends-on ("defprimitive"))
                 (:file "int" :depends-on ("zigzag" "defprimitive"))
                 (:file "long" :depends-on ("zigzag" "defprimitive"))
                 (:file "float" :depends-on ("ieee-754" "defprimitive"))
                 (:file "double" :depends-on ("ieee-754" "defprimitive"))
                 (:file "bytes" :depends-on ("long" "defprimitive" "compare"))
                 (:file "string" :depends-on ("bytes" "long" "defprimitive"))))
   (:file "schema"
    :depends-on ("primitive"
                 "api"
                 "little-endian"
                 "type"
                 "crc-64-avro"
                 "recursive-descent"))
   (:module "name"
    :depends-on ("api" "mop" "recursive-descent" "schema" "type" "ascii")
    :components ((:file "type")
                 (:file "deduce" :depends-on ("type"))
                 (:file "class" :depends-on ("type" "deduce"))
                 (:file "schema" :depends-on ("type" "deduce" "class"))
                 (:file "coerce" :depends-on ("deduce" "schema"))
                 (:file "package"
                  :depends-on ("type" "deduce" "class" "schema" "coerce"))))
   (:module "complex"
    :depends-on ("type" "api" "mop" "recursive-descent" "schema" "name")
    :components ((:file "count-and-size")
                 (:file "array" :depends-on ("count-and-size"))
                 (:file "map" :depends-on ("count-and-size"))
                 (:file "union")
                 (:file "fixed")
                 (:file "enum")
                 (:file "record")))
   (:module "logical"
    :depends-on ("type"
                 "mop"
                 "ascii"
                 "api"
                 "recursive-descent"
                 "primitive"
                 "schema"
                 "complex")
    :components ((:file "uuid")
                 (:file "datetime")
                 (:file "date" :depends-on ("datetime"))
                 (:file "time-millis" :depends-on ("datetime"))
                 (:file "time-micros" :depends-on ("datetime"))
                 (:file "timestamp-millis" :depends-on ("datetime"))
                 (:file "timestamp-micros" :depends-on ("datetime"))
                 (:file "local-timestamp-millis" :depends-on ("datetime"))
                 (:file "local-timestamp-micros" :depends-on ("datetime"))
                 (:file "big-endian")
                 (:file "decimal" :depends-on ("big-endian"))
                 (:file "duration")))
   (:module "file"
    :depends-on ("primitive" "complex" "logical")
    :components ((:file "file-header")
                 (:file "file-block" :depends-on ("file-header"))
                 (:file "file-reader" :depends-on ("file-block"))
                 (:file "file-writer" :depends-on ("file-block"))))
   (:module "ipc"
    :depends-on ("primitive" "complex" "logical")
    :components ((:file "handshake")
                 (:file "error" :depends-on ("handshake"))
                 (:file "message" :depends-on ("error"))
                 (:file "framing" :depends-on ("handshake"))
                 (:file "parse" :depends-on ("error" "message"))
                 (:file "protocol" :depends-on ("parse" "handshake"))
                 (:file "client" :depends-on ("framing" "protocol"))
                 (:file "protocol-object" :depends-on ("client"))
                 (:file "server"
                  :depends-on ("framing" "protocol" "protocol-object"))))))

(defsystem #:cl-avro/test
  :description "Tests for cl-avro."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :pathname "test"
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :depends-on (#:cl-avro #:1am #:flexi-streams #:named-readtables #:babel)
  :components ((:file "common")
               (:file "compare")
               (:file "file")
               (:module "complex"
                :depends-on ("common")
                :components ((:file "array")
                             (:file "enum")
                             (:file "fixed")
                             (:file "map")
                             (:file "record")
                             (:file "union")))
               (:module "logical"
                :depends-on ("common")
                :components ((:file "date")
                             (:file "decimal")
                             (:file "duration")
                             (:file "local-timestamp-micros")
                             (:file "local-timestamp-millis")
                             (:file "time-micros")
                             (:file "time-millis")
                             (:file "timestamp-micros")
                             (:file "timestamp-millis")
                             (:file "uuid")))
               (:module "primitive"
                :depends-on ("common")
                :components ((:file "boolean")
                             (:file "bytes")
                             (:file "double")
                             (:file "float")
                             (:file "int")
                             (:file "long")
                             (:file "null")
                             (:file "string")))
               (:module "resolution"
                :components ((:file "base")
                             (:file "primitive"
                              :depends-on ("base"))
                             (:file "promote"
                              :depends-on ("base"))
                             (:module "complex"
                              :depends-on ("base")
                              :components ((:file "array")
                                           (:file "enum")
                                           (:file "fixed")
                                           (:file "map")
                                           (:file "record")
                                           (:file "union")))
                             (:module "logical"
                              :depends-on ("base")
                              :components ((:file "date")
                                           (:file "decimal")
                                           (:file "duration")
                                           (:file "local-timestamp-micros")
                                           (:file "local-timestamp-millis")
                                           (:file "time-micros")
                                           (:file "time-millis")
                                           (:file "timestamp-micros")
                                           (:file "timestamp-millis")
                                           (:file "uuid")))))
               (:module "ipc"
                :components ((:file "common")
                             (:file "stateless"
                              :depends-on ("common"))
                             (:file "stateful"
                              :depends-on ("common"))))))
