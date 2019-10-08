;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/resolve
  (:use #:cl #:1am))

(in-package #:test/resolve)

(test resolve-enum
  (let ((writer-schema (avro:json->schema
                        "{type: \"enum\",
                          name: \"EnumName\",
                          symbols: [\"FOO\", \"BAR\", \"BAZ\"],
                          default: \"BAZ\"}"))
        (reader-schema (avro:json->schema
                        "{type: \"enum\",
                          name: \"EnumName\",
                          symbols: [\"FOO\", \"BAR\"],
                          default: \"FOO\"}")))
    (is (string=
         "BAR"
         (avro:deserialize (avro:serialize nil writer-schema "BAR")
                           reader-schema)))
    (is (string=
         "BAR"
         (avro:deserialize (avro:serialize nil writer-schema "BAR")
                           reader-schema
                           writer-schema)))
    (is (string=
         "FOO"
         (avro:deserialize (avro:serialize nil writer-schema "BAZ")
                           reader-schema
                           writer-schema)))))

(test resolve-record
  (let ((writer-schema (avro:json->schema
                        "{type: \"record\",
                          name: \"RecordName\",
                          fields: [
                            {name: \"Field_1\", type: \"string\"},
                            {name: \"Field_2\", type: \"int\"},
                            {name: \"UnknownField\",
                             type: \"string\",
                             default: \"baz\"}]}"))
        (reader-schema (avro:json->schema
                        "{type: \"record\",
                          name: \"RecordName2\",
                          aliases: [\"RecordName\"],
                          fields: [
                            {name: \"Field_2\", type: \"int\", default: 4},
                            {name: \"Field_11\",
                             aliases: [\"FooField\", \"Field_1\"],
                             type: \"string\"},
                            {name: \"Field_3\",
                             type: \"string\",
                             default: \"bar\"}]}")))
    (is
     (equal
      '(2 "foo" "bar")
      (coerce
       (avro:deserialize (avro:serialize nil writer-schema '("foo" 2 "ignored"))
                         reader-schema
                         writer-schema)
       'list)))))
