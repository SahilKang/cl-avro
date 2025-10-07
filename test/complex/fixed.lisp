;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro/test/fixed
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/fixed)

(named-readtables:in-readtable json-syntax)

(define-schema-test schema
  {
    "type": "fixed",
    "name": "FixedName",
    "namespace": "fixed.name.space",
    "aliases": ["foo", "foo.bar"],
    "size": 12
  }
  {
    "name": "fixed.name.space.FixedName",
    "type": "fixed",
    "size": 12
  }
  #x606d48807f193010
  (make-instance
   'avro:fixed
   :name '|FixedName|
   :namespace "fixed.name.space"
   :aliases '("foo" "foo.bar")
   :size 12)
  (defclass |FixedName| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "fixed.name.space")
    (:aliases "foo" "foo.bar")
    (:size 12)))

(define-io-test io
    ((expected '(2 4 6)))
    (make-instance 'avro:fixed :name 'fixed :size 3)
    (make-instance schema :initial-contents expected)
    (2 4 6)
  (is (equal expected (coerce arg 'list))))

(define-schema-test python-test
  {
    "type": "fixed",
    "name": "Test",
    "size": 1
  }
  {
    "name": "Test",
    "type": "fixed",
    "size": 1
  }
  #x5b3549407b896968
  (make-instance
   'avro:fixed
   :name "Test"
   :size 1)
  (defclass |Test| ()
    ()
    (:metaclass avro:fixed)
    (:size 1)))

(define-schema-test python-my-fixed
  {
    "type": "fixed",
    "name": "MyFixed",
    "namespace": "org.apache.hadoop.avro",
    "size": 1
  }
  {
    "name": "org.apache.hadoop.avro.MyFixed",
    "type": "fixed",
    "size": 1
  }
  #x45df5be838d1dbfa
  (make-instance
   'avro:fixed
   :name '|MyFixed|
   :namespace "org.apache.hadoop.avro"
   :size 1)
  (defclass |MyFixed| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "org.apache.hadoop.avro")
    (:size 1)))
