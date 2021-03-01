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

(defpackage #:test/fingerprint
  (:use #:cl #:1am))

(in-package #:test/fingerprint)

(test int-fingerprint
  (let ((schema (avro:deserialize 'avro:schema "\"int\""))
        (expected #x7275d51a3f395c8f))
    (is (= expected (avro:fingerprint64 schema)))))

(test needs-moar-int
  (let ((schema (avro:deserialize 'avro:schema "{type: \"int\"}"))
        (expected #x7275d51a3f395c8f))
    (is (= expected (avro:fingerprint64 schema)))))

(test float-fingerprint
  (let ((schema (avro:deserialize 'avro:schema "\"float\""))
        (expected #x4d7c02cb3ea8d790))
    (is (= expected (avro:fingerprint64 schema)))))

(test long-fingerprint
  (let ((schema (avro:deserialize 'avro:schema "\"long\""))
        (expected #xd054e14493f41db7))
    (is (= expected (avro:fingerprint64 schema)))))

(test double-fingerprint
  (let ((schema (avro:deserialize 'avro:schema "\"double\""))
        (expected #x8e7535c032ab957e))
    (is (= expected (avro:fingerprint64 schema)))))

(test fixed-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"fixed\",
                   name: \"MyFixed\",
                   namespace: \"org.apache.hadoop.avro\",
                   size: 1}"))
        (expected #x45df5be838d1dbfa))
    (is (= expected (avro:fingerprint64 schema)))))

(test enum-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"enum\",
                   name: \"Test\",
                   \"symbols\": [\"A\", \"B\"]}"))
        (expected #x167a7fe2c2f2a203))
    (is (= expected (avro:fingerprint64 schema)))))

(test array-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"array\",
                   items: {type: \"enum\",
                           name: \"Test\",
                           symbols: [\"A\", \"B\"]}}"))
        (expected #x87033afae1add910))
    (is (= expected (avro:fingerprint64 schema)))))

(test map-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"map\",
                   values: {type: \"enum\",
                            name: \"Test\",
                            symbols: [\"A\", \"B\"]}}"))
        (expected #x2d816b6f62b02adf))
    (is (= expected (avro:fingerprint64 schema)))))

(test union-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "[\"string\", \"null\", \"long\"]"))
        (expected #x6675680d41bea565))
    (is (= expected (avro:fingerprint64 schema)))))

(test record-fingerprint
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"Test\",
                   fields: [{name: \"f\", type: \"long\"}]}"))
        (expected #x8e58ebe6f5e594ed))
    (is (= expected (avro:fingerprint64 schema)))))

(test another-record
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"Node\",
                   fields: [
                     {name: \"label\", type: \"string\"},
                     {name: \"children\",
                      type: {type: \"array\", items: \"Node\"}}]}"))
        (expected #xb756e7c344a5cb52))
    (is (= expected (avro:fingerprint64 schema)))))

(test lisp-record
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"Lisp\",
                   fields: [
                     {name: \"value\",
                      type: [
                        \"null\",
                        \"string\",
                        {type: \"record\",
                         name: \"Cons\",
                         fields: [{name: \"car\", type: \"Lisp\"},
                                  {name: \"cdr\", type: \"Lisp\"}]}]}]}"))
        (expected #x06b3a0ed231ad968))
    (is (= expected (avro:fingerprint64 schema)))))

(test handshake-request
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"HandshakeRequest\",
                   namespace: \"org.apache.avro.ipc\",
                   fields: [
                     {name: \"clientHash\",
                      type: {type: \"fixed\", name: \"MD5\", size: 16}},
                     {name: \"clientProtocol\", type: [\"null\", \"string\"]},
                     {name: \"serverHash\", type: \"MD5\"},
                     {name: \"meta\",
                      type: [\"null\", {type: \"map\", values: \"bytes\"}]}]}"))
        (expected #x57577c5a9ed76ab9))
    (is (= expected (avro:fingerprint64 schema)))))

(test handshake-response
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"HandshakeResponse\",
                   namespace: \"org.apache.avro.ipc\",
                   fields: [
                     {name: \"match\",
                      type: {
                        type: \"enum\",
                        name: \"HandshakeMatch\",
                        symbols: [\"BOTH\", \"CLIENT\", \"NONE\"]
                      }
                     },
                     {name: \"serverProtocol\", type: [\"null\", \"string\"]},
                     {name:\"serverHash\",
                      type: [\"null\",
                             {name: \"MD5\", size: 16, type: \"fixed\"}]
                     },
                     {name: \"meta\",
                      type: [\"null\", {type: \"map\", values: \"bytes\"}]}]}"))
        (expected #x0ea54ede01eefe00))
    (is (= expected (avro:fingerprint64 schema)))))

(test kitchen-sink
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{
      \"type\": \"record\",
      \"name\": \"Interop\",
      \"namespace\": \"org.apache.avro\",
      \"fields\": [
        {\"name\": \"intField\", \"type\": \"int\"},
        {\"name\": \"longField\", \"type\": \"long\"},
        {\"name\": \"stringField\", \"type\": \"string\"},
        {\"name\": \"boolField\", \"type\": \"boolean\"},
        {\"name\": \"floatField\", \"type\": \"float\"},
        {\"name\": \"doubleField\", \"type\": \"double\"},
        {\"name\": \"bytesField\", \"type\": \"bytes\"},
        {\"name\": \"nullField\", \"type\": \"null\"},
        {\"name\": \"arrayField\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},
        {
          \"name\": \"mapField\",
          \"type\": {
            \"type\": \"map\",
            \"values\": {\"name\": \"Foo\",
                       \"type\": \"record\",
                       \"fields\": [{\"name\": \"label\", \"type\": \"string\"}]}
          }
        },
        {
          \"name\": \"unionField\",
          \"type\": [\"boolean\", \"double\", {\"type\": \"array\", \"items\": \"bytes\"}]
        },
        {
          \"name\": \"enumField\",
          \"type\": {\"type\": \"enum\", \"name\": \"Kind\", \"symbols\": [\"A\", \"B\", \"C\"]}
        },
        {
          \"name\": \"fixedField\",
          \"type\": {\"type\": \"fixed\", \"name\": \"MD5\", \"size\": 16}
        },
        {
          \"name\": \"recordField\",
          \"type\": {\"type\": \"record\",
                   \"name\": \"Node\",
                   \"fields\": [{\"name\": \"label\", \"type\": \"string\"},
                              {\"name\": \"children\",
                               \"type\": {\"type\": \"array\",
                                        \"items\": \"Node\"}}]}
        }
      ]
    }"))
        (expected #xa4b5a0a6930a2ce8))
    (is (= expected (avro:fingerprint64 schema)))))

(test ip-address
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"ipAddr\",
                   fields: [
                     {name: \"addr\",
                      type: [
                        {name: \"IPv6\", type: \"fixed\", size: 16},
                        {name: \"IPv4\", type: \"fixed\", size: 4}]}]}"))
        (expected #x44188a294e1b968d))
    (is (= expected (avro:fingerprint64 schema)))))

(test docstring
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"TestDoc\",
                   doc: \"Doc String\",
                   fields: [
                     {name: \"name\", type: \"string\", doc: \"Doc String\"}]}"))
        (expected #x09c1cd2bf060660e))
    (is (= expected (avro:fingerprint64 schema)))))

(test another-docstring
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"enum\",
                   name: \"Test\",
                   symbols: [\"A\", \"B\"],
                   doc: \"Doc String\"}"))
        (expected #x167a7fe2c2f2a203))
    (is (= expected (avro:fingerprint64 schema)))))
