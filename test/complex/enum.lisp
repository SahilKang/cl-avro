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

(defpackage #:test/enum
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))

(in-package #:test/enum)

(named-readtables:in-readtable json-syntax)

(define-schema-test python-test
  {
    "type": "enum",
    "name": "Test",
    "symbols": ["A", "B"]
  }
  {
    "name": "Test",
    "type": "enum",
    "symbols": ["A", "B"]
  }
  #x167a7fe2c2f2a203
  (make-instance
   'avro:enum
   :name "Test"
   :symbols '("A" "B"))
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B")))

(define-schema-test python-test-with-doc
  {
    "type": "enum",
    "name": "Test",
    "symbols": ["A", "B"],
    "doc": "Doc String"
  }
  {
    "name": "Test",
    "type": "enum",
    "symbols": ["A", "B"],
  }
  #x167a7fe2c2f2a203
  (make-instance
   'avro:enum
   :name '|Test|
   :symbols '("A" "B")
   :documentation "Doc String")
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B")
    (:documentation "Doc String")))

(define-schema-test all-fields
  {
    "type": "enum",
    "name": "baz",
    "namespace": "foo.bar",
    "aliases": ["some", "foo.bar.alias"],
    "doc": "Here's a doc",
    "symbols": ["FOO", "BAR", "BAZ"],
    "default": "BAR"
  }
  {
    "name": "foo.bar.baz",
    "type": "enum",
    "symbols": ["FOO", "BAR", "BAZ"]
  }
  #x2d77464e9fd3b6b5
  (make-instance
   'avro:enum
   :name "baz"
   :namespace "foo.bar"
   :aliases '("some" "foo.bar.alias")
   :documentation "Here's a doc"
   :symbols '("FOO" "BAR" "BAZ")
   :default "BAR")
  (defclass |baz| ()
    ()
    (:metaclass avro:enum)
    (:namespace "foo.bar")
    (:aliases "some" "foo.bar.alias")
    (:documentation "Here's a doc")
    (:symbols "FOO" "BAR" "BAZ")
    (:default "BAR")))

(define-schema-test optional-fields
  {
    "type": "enum",
    "name": "EnumName",
    "aliases": [],
    "default": "FOO",
    "symbols": [
      "FOO",
      "BAR",
      "BAZ"
    ]
  }
  {
    "name": "EnumName",
    "type": "enum",
    "symbols": [
      "FOO",
      "BAR",
      "BAZ"
    ]
  }
  #xcda0ac7bef77b89c
  (make-instance
   'avro:enum
   :name "EnumName"
   :aliases nil
   :default "FOO"
   :symbols #("FOO" "BAR" "BAZ"))
  (defclass |EnumName| ()
    ()
    (:metaclass avro:enum)
    (:aliases)
    (:default "FOO")
    (:symbols "FOO" "BAR" "BAZ")))

(define-io-test io
    ((expected "B"))
    (make-instance 'avro:enum :name "foo" :symbols '("A" "B"))
    (make-instance schema :enum expected)
    (2)
  (is (string= expected (avro:which-one arg)))
  (signals error
    (make-instance schema :enum "C")))
