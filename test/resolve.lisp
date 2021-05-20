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

(in-package #:cl-user)

(defpackage #:test/resolve
  (:use #:cl #:1am))

(in-package #:test/resolve)

(declaim
 (ftype (function (avro:record-object simple-string)
                  (values avro:object &optional))
        field))
(defun field (record field)
  (let ((found-field (find field (avro:fields (class-of record))
                           :key #'avro:name :test #'string=)))
    (unless found-field
      (error "No such field ~S" field))
    (slot-value record (nth-value 1 (avro:name found-field)))))


(test resolve-enum
  (let ((writer-schema (avro:deserialize
                        'avro:schema
                        "{type: \"enum\",
                          name: \"EnumName\",
                          symbols: [\"FOO\", \"BAR\", \"BAZ\"],
                          default: \"BAZ\"}"))
        (reader-schema (avro:deserialize
                        'avro:schema
                        "{type: \"enum\",
                          name: \"EnumName\",
                          symbols: [\"FOO\", \"BAR\"],
                          default: \"FOO\"}")))

    (is (string=
         "BAR"
         (avro:which-one
          (avro:deserialize
           writer-schema
           (avro:serialize (make-instance writer-schema :enum "BAR"))
           :reader-schema reader-schema))))

    (is (string=
         "FOO"
         (avro:which-one
          (avro:deserialize
           writer-schema
           (avro:serialize (make-instance writer-schema :enum "BAZ"))
           :reader-schema reader-schema))))))

(test resolve-record
  (let ((writer-schema (avro:deserialize
                        'avro:schema
                        "{type: \"record\",
                          name: \"RecordName\",
                          fields: [
                            {name: \"Field_1\", type: \"string\"},
                            {name: \"Field_2\", type: \"int\"},
                            {name: \"UnknownField\",
                             type: \"string\",
                             default: \"baz\"}]}"))
        (reader-schema (avro:deserialize
                        'avro:schema
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
      (let ((object (avro:deserialize
                     writer-schema
                     (avro:serialize (make-instance
                                      writer-schema
                                      :|Field_1| "foo"
                                      :|Field_2| 2
                                      :|UnknownField| "ignored"))
                     :reader-schema reader-schema)))
        (list
         (field object "Field_2")
         (field object "Field_11")
         (field object "Field_3")))))))
