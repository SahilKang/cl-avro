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

(defpackage #:test/single-object-encoding
  (:use #:cl #:1am))

(in-package #:test/single-object-encoding)

(test null-object
  (let* ((expected nil)
         (schema (avro:schema-of expected))
         (single-object (avro:write-single-object expected)))
    (is (avro:single-object-p single-object))
    (is (= (avro:fingerprint schema)
           (avro:single-object->fingerprint single-object)))
    (is (eq expected (avro:deserialize-single-object schema single-object)))))

(test int-object
  (let* ((expected 24601)
         (schema (avro:schema-of expected))
         (single-object (avro:write-single-object expected)))
    (is (avro:single-object-p single-object))
    (is (= (avro:fingerprint schema)
           (avro:single-object->fingerprint single-object)))
    (is (= expected (avro:deserialize-single-object schema single-object)))))

(test enum-object
  (let* ((expected "BAR")
         (schema (avro:deserialize
                  'avro:schema
                  "{type: \"enum\",
                    name: \"Name\",
                    symbols: [\"FOO\", \"BAR\", \"BAZ\"]}"))
         (single-object (avro:write-single-object
                         (make-instance schema :enum expected))))
    (is (avro:single-object-p single-object))
    (is (= (avro:fingerprint schema)
           (avro:single-object->fingerprint single-object)))
    (is (string= expected
                 (avro:which-one
                  (avro:deserialize-single-object schema single-object))))))

(test record-object
  (let* ((expected '("Hello" "World" "!" 2 4 6))
         (schema (avro:deserialize
                  'avro:schema
                  "{type: \"record\",
                    name: \"Name\",
                    fields: [
                      {type: \"string\", name: \"field1\"},
                      {type: \"string\", name: \"field2\"},
                      {type: \"string\", name: \"field3\"},
                      {type: \"int\", name: \"field4\"},
                      {type: \"int\", name: \"field5\"},
                      {type: \"int\", name: \"field6\"}
                    ]}"))
         (single-object
           (avro:write-single-object
            (apply #'make-instance schema
                   (mapcan (lambda (field-name value)
                             (list (intern field-name 'keyword) value))
                           (map 'list #'avro:name (avro:fields schema))
                           expected)))))
    (is (avro:single-object-p single-object))
    (is (= (avro:fingerprint schema)
           (avro:single-object->fingerprint single-object)))
    (is (equal expected
               (let* ((record (avro:deserialize-single-object
                               schema single-object)))
                 (flet ((field (field)
                          (slot-value record (nth-value 1 (avro:name field)))))
                   (map 'list #'field (avro:fields (class-of record)))))))))
