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

(defpackage #:test/single-object-encoding
  (:use #:cl #:1am))

(in-package #:test/single-object-encoding)

(test check-predicate-with-turds
  (is (not (avro:single-object-p nil)))
  (is (not (avro:single-object-p #(2 4 6 8))))
  (is (not (avro:single-object-p #(#xc3 #x01))))
  (is (not (avro:single-object-p #(#xc3 #x01 2 4 6 8 'who-do-we-appreciate)))))

(test null-object
  (let* ((expected nil)
         (schema (avro:json->schema "\"null\""))
         (single-object (avro:write-single-object schema expected)))
    (is (avro:single-object-p single-object))
    (destructuring-bind (fingerprint payload)
        (avro:read-single-object single-object)
      (is (= fingerprint (avro:fingerprint schema #'avro:avro-64bit-crc)))
      (is (eq expected (avro:deserialize payload schema))))))

(test int-object
  (let* ((expected 24601)
         (schema (avro:json->schema "\"int\""))
         (single-object (avro:write-single-object schema expected)))
    (is (avro:single-object-p single-object))
    (destructuring-bind (fingerprint payload)
        (avro:read-single-object single-object)
      (is (= fingerprint (avro:fingerprint schema #'avro:avro-64bit-crc)))
      (is (= expected (avro:deserialize payload schema))))))

(test enum-object
  (let* ((expected "BAR")
         (schema (avro:json->schema
                  "{type: \"enum\",
                    name: \"Name\",
                    symbols: [\"FOO\", \"BAR\", \"BAZ\"]}"))
         (single-object (avro:write-single-object schema expected)))
    (is (avro:single-object-p single-object))
    (destructuring-bind (fingerprint payload)
        (avro:read-single-object single-object)
      (is (= fingerprint (avro:fingerprint schema #'avro:avro-64bit-crc)))
      (is (string= expected (avro:deserialize payload schema))))))

(test record-object
  (let* ((expected '("Hello" "World" "!" 2 4 6))
         (schema (avro:json->schema
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
         (single-object (avro:write-single-object schema expected)))
    (is (avro:single-object-p single-object))
    (destructuring-bind (fingerprint payload)
        (avro:read-single-object single-object)
      (is (= fingerprint (avro:fingerprint schema #'avro:avro-64bit-crc)))
      (is (equal expected (coerce (avro:deserialize payload schema) 'list))))))
