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

(asdf:defsystem #:cl-avro
  :description "Implementation of the Apache Avro data serialization system."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:uiop
               #:flexi-streams
               #:babel
               #:local-time
               #:local-time-duration
               #:time-interval
               #:ieee-floats
               #:st-json
               #:chipz
               #:salza2
               #:closer-mop
               #:trivial-extensible-sequences
               #:genhash)
  :in-order-to ((test-op (test-op #:cl-avro/test)))
  :build-pathname "cl-avro"
  :pathname "src"
  :components
  ((:module "schema"
    :components ((:file "primitive")
                 (:file "ascii")
                 (:module "complex"
                  :depends-on ("primitive" "ascii")
                  :components ((:file "common")
                               (:file "base")
                               (:module "named"
                                :depends-on ("common" "base")
                                :serial t
                                :components ((:file "type")
                                             (:file "schema")
                                             (:file "package")))
                               (:file "array"
                                :depends-on ("base"))
                               (:file "enum"
                                :depends-on ("common" "base" "named"))
                               (:file "fixed"
                                :depends-on ("base" "named"))
                               (:file "map"
                                :depends-on ("base"))
                               (:file "union"
                                :depends-on ("common" "base" "named"))
                               (:module "record"
                                :depends-on ("common"
                                             "base"
                                             "named"
                                             "array"
                                             "enum"
                                             "fixed"
                                             "map"
                                             "union")
                                :serial t
                                :components ((:file "notation")
                                             (:file "field")
                                             (:file "effective-field")
                                             (:file "schema")
                                             (:file "package")))
                               (:file "package"
                                :depends-on ("base"
                                             "named"
                                             "array"
                                             "enum"
                                             "fixed"
                                             "map"
                                             "union"
                                             "record"))))
                 (:module "logical"
                  :depends-on ("primitive" "ascii" "complex")
                  :components ((:file "base")
                               (:file "uuid"
                                :depends-on ("base"))
                               (:file "decimal"
                                :depends-on ("base"))
                               (:file "duration"
                                :depends-on ("base"))
                               (:file "timezone")
                               (:file "date"
                                :depends-on ("base" "timezone"))
                               (:file "time"
                                :depends-on ("base" "timezone"))
                               (:file "timestamp"
                                :depends-on ("date" "time"))
                               (:file "local-timestamp"
                                :depends-on ("date" "time"))
                               (:file "package"
                                :depends-on ("base"
                                             "uuid"
                                             "decimal"
                                             "duration"
                                             "timezone"
                                             "date"
                                             "time"
                                             "timestamp"
                                             "local-timestamp"))))
                 (:module "io"
                  :depends-on ("primitive" "complex" "logical")
                  :components ((:file "common")
                               (:file "st-json")
                               (:file "read"
                                :depends-on ("common" "st-json"))
                               (:module "write"
                                :depends-on ("common" "st-json")
                                :serial t
                                :components ((:file "canonicalize")
                                             (:file "write")))
                               (:file "package"
                                :depends-on ("read" "write"))))
                 (:file "fingerprint"
                  :depends-on ("complex" "io"))
                 (:file "schema-of"
                  :depends-on ("primitive" "complex"))
                 (:file "package"
                  :depends-on ("primitive"
                               "complex"
                               "logical"
                               "io"
                               "fingerprint"
                               "schema-of"))))
   (:module "io"
    :depends-on ("schema")
    :components ((:file "base")
                 (:file "schema"
                  :depends-on ("base"))
                 (:file "primitive"
                  :depends-on ("base"))
                 (:file "complex"
                  :depends-on ("base"))
                 (:file "logical"
                  :depends-on ("base" "primitive"))
                 (:file "compare"
                  :depends-on ("base"))
                 (:module "resolution"
                  :depends-on ("base")
                  :components ((:file "assert-match")
                               (:file "resolve"
                                :depends-on ("assert-match"))
                               (:file "promoted"
                                :depends-on ("assert-match" "resolve"))
                               (:file "logical"
                                :depends-on ("assert-match" "resolve"))
                               (:file "package"
                                :depends-on ("resolve" "logical" "promoted"))))
                 (:file "package"
                  :depends-on ("schema"
                               "primitive"
                               "complex"
                               "logical"
                               "resolution"
                               "compare"))))
   (:file "single-object-encoding"
    :depends-on ("schema" "io"))
   (:module "object-container-file"
    :depends-on ("schema" "io")
    :components ((:file "header")
                 (:file "file-block"
                  :depends-on ("header"))
                 (:file "file-input-stream"
                  :depends-on ("header" "file-block"))
                 (:file "file-output-stream"
                  :depends-on ("header" "file-block"))
                 (:file "package"
                  :depends-on ("header"
                               "file-block"
                               "file-input-stream"
                               "file-output-stream"))))
   (:file "package"
    :depends-on ("schema"
                 "io"
                 "single-object-encoding"
                 "object-container-file"))))


(asdf:defsystem #:cl-avro/test
  :description "Tests for cl-avro."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:cl-avro #:1am #:flexi-streams #:named-readtables)
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :pathname "test"
  :components
  ((:file "common")
   (:file "compare")
   (:file "object-container-file")
   (:file "resolve")
   (:file "single-object-encoding")
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
                 (:file "string")))))
