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

(defpackage #:test/union
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:define-schema-test
                #:json-syntax))

(in-package #:test/union)

(named-readtables:in-readtable json-syntax)

(define-schema-test python-test
  ["string", "null", "long"]
  ["string", "null", "long"]
  #x6675680d41bea565
  (make-instance
   'avro:union
   :schemas '(avro:string avro:null avro:long))
  (defclass union<string-null-long> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string avro:null avro:long)))

(define-schema-test fixed-union
  [
    "string",
    {
      "type": "fixed",
      "name": "baz",
      "namespace": "foo.bar",
      "size": 12
    }
  ]
  [
    "string",
    {
      "name": "foo.bar.baz",
      "type": "fixed",
      "size": 12
    }
  ]
  #x370f6358da669947
  (make-instance
   'avro:union
   :schemas (list
             'avro:string
             (make-instance
              'avro:fixed
              :name '|baz|
              :namespace "foo.bar"
              :size 12)))
  (defclass |baz| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "foo.bar")
    (:size 12))
  (defclass union<string-foo.bar.baz> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string |baz|)))

(test io
  (let* ((schema (make-instance 'avro:union :schemas '(avro:null avro:string)))
         (expected "foobar")
         (object (make-instance schema :object expected))
         (serialized
           (make-array
            8
            :element-type '(unsigned-byte 8)
            :initial-contents '(2 12 #x66 #x6f #x6f #x62 #x61 #x72))))
    (is (string= expected (avro:object object)))
    (is (equalp serialized (avro:serialize object)))
    (let ((deserialized (avro:deserialize schema serialized)))
      (is (eq schema (avro:schema-of deserialized)))
      (is (string= expected (avro:object deserialized))))
    (is (null (avro:object (make-instance schema :object nil))))
    (signals error
      (make-instance schema :object 3))))

(test late-type-check
  (setf (find-class 'late_union) nil
        (find-class 'late_fixed) nil)

  (defclass late_union ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string late_fixed))

  (signals error
    (avro:schemas (find-class 'late_union)))

  (defclass late_fixed ()
    ()
    (:metaclass avro:fixed)
    (:size 12))

  (is (eq (find-class 'late_fixed)
          (elt (avro:schemas (find-class 'late_union)) 1))))

(test no-schemas
  (signals error
    (make-instance
     'avro:union
     :schemas #())))

(test non-schema
  (setf (find-class 'some_union) nil
        (find-class 'some_class) nil)

  (defclass some_class ()
    ())

  (defclass some_union ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string some_class))

  (signals error
    (avro:schemas (find-class 'some_union))))

(test duplicate-primitive
  (let ((schema (make-instance
                 'avro:union
                 :schemas '(avro:string avro:null avro:string))))
    (signals error
      (avro:schemas schema))))

(test duplicate-array
  (let* ((array<int> (make-instance 'avro:array :items 'avro:int))
         (array<string> (make-instance 'avro:array :items 'avro:string))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null array<int> array<string>))))
    (signals error
      (avro:schemas schema))))

(test duplicate-named
  (let* ((fixed (make-instance 'avro:fixed :name "foo" :size 12))
         (enum (make-instance 'avro:enum :name "foo" :symbols '("FOO" "BAR")))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null fixed enum))))
    (signals error
      (avro:schemas schema))))

(test different-name-same-type
  (let* ((fixed-1 (make-instance 'avro:fixed :name "foo" :size 12))
         (fixed-2 (make-instance 'avro:fixed :name "bar" :size 12))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null fixed-1 fixed-2))))
    (is (equalp (vector 'avro:null fixed-1 fixed-2)
                (avro:schemas schema)))))
