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

(defpackage #:test/array
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:define-schema-test
                #:json-syntax))

(in-package #:test/array)

(named-readtables:in-readtable json-syntax)

;; TODO need to support default
(define-schema-test long-array
  {
    "type": "array",
    "items": "long"
  }
  {
    "type": "array",
    "items": "long"
  }
  #x5416c98ba22e5e71
  (make-instance
   'avro:array
   :items 'avro:long)
  (defclass long_array ()
    ()
    (:metaclass avro:array)
    (:items avro:long)))

(define-schema-test enum-array
  {
    "type": "array",
    "items": {
      "type": "enum",
      "name": "Test",
      "symbols": ["A", "B"]
    }
  }
  {
    "type": "array",
    "items": {
      "name": "Test",
      "type": "enum",
      "symbols": ["A", "B"]
    }
  }
  #x87033afae1add910
  (make-instance
   'avro:array
   :items (make-instance
           'avro:enum
           :name "Test"
           :symbols '("A" "B")))
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B"))
  (defclass enum_array ()
    ()
    (:metaclass avro:array)
    (:items |Test|)))

(define-schema-test record-array
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "RecordName",
      "fields": [
        {
          "name": "Foo",
          "type": "string",
          "default": "foo"
        },
        {
          "name": "Bar",
          "type": "int",
          "default": 7
        }
      ]
    }
  }
  {
    "type": "array",
    "items": {
      "name": "RecordName",
      "type": "record",
      "fields": [
        {
          "name": "Foo",
          "type": "string",
        },
        {
          "name": "Bar",
          "type": "int",
        }
      ]
    }
  }
  #x2a6ad7a6c7ab28ea
  (make-instance
   'avro:array
   :items (make-instance
           'avro:record
           :name "RecordName"
           :direct-slots
           `((:name |Foo| :type avro:string :default "foo")
             (:name |Bar| :type avro:int :default 7))))
  (defclass |RecordName| ()
    ((|Foo| :type avro:string :default "foo")
     (|Bar| :type avro:int :default 7))
    (:metaclass avro:record))
  (defclass array<record-name> ()
    ()
    (:metaclass avro:array)
    (:items |RecordName|)))

(test io
  (let* ((enum-schema
           (make-instance 'avro:enum :name "Test" :symbols '("A" "B")))
         (array-schema
           (make-instance 'avro:array :items enum-schema))
         (expected
           '("A" "A" "B"))
         (object
           (make-instance
            array-schema
            :initial-contents (mapcar
                               (lambda (enum)
                                 (make-instance enum-schema :enum enum))
                               expected)))
         (serialized
           (make-array
            5
            :element-type '(unsigned-byte 8)
            :initial-contents '(6 0 0 2 0))))
    (is (equal expected (map 'list #'avro:which-one object)))
    (is (equalp serialized (avro:serialize object)))
    (let ((deserialized (avro:deserialize array-schema serialized)))
      (is (eq array-schema (class-of deserialized)))
      (is (equal expected (map 'list #'avro:which-one deserialized))))))
