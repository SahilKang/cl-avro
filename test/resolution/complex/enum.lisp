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
(defpackage #:test/resolution/enum
  (:use #:cl #:1am))
(in-package #:test/resolution/enum)

(test no-reader-default
  (let* ((writer-enum (make-instance
                       'avro:enum
                       :name "foo"
                       :aliases '("baz" "foo.bar")
                       :default "BAZ"
                       :symbols '("FOO" "BAR" "BAZ")))
         (reader-enum (make-instance
                       'avro:enum
                       :name "foobar"
                       :aliases '("bar.foo")
                       :symbols '("FOO" "BAR")))
         (foo (make-instance writer-enum :enum "FOO"))
         (bar (make-instance writer-enum :enum "BAR"))
         (baz (make-instance writer-enum :enum "BAZ")))
    (is (typep foo writer-enum))
    (let ((foo (avro:coerce
                (avro:deserialize writer-enum (avro:serialize foo))
                reader-enum)))
      (is (typep foo reader-enum))
      (is (string= "FOO" (avro:which-one foo))))

    (is (typep bar writer-enum))
    (let ((bar (avro:coerce
                (avro:deserialize writer-enum (avro:serialize bar))
                reader-enum)))
      (is (typep bar reader-enum))
      (is (string= "BAR" (avro:which-one bar))))

    (is (typep baz writer-enum))
    (signals error
      (avro:coerce
       (avro:deserialize writer-enum (avro:serialize baz))
       reader-enum))))

(test reader-default
  (let* ((writer-enum (make-instance
                       'avro:enum
                       :name "foo"
                       :aliases '("baz" "foo.bar")
                       :default "BAZ"
                       :symbols '("FOO" "BAR" "BAZ")))
         (reader-enum (make-instance
                       'avro:enum
                       :name "foobar"
                       :aliases '("bar.foo")
                       :symbols '("FOO" "BAR")
                       :default "BAR"))
         (foo (make-instance writer-enum :enum "FOO"))
         (bar (make-instance writer-enum :enum "BAR"))
         (baz (make-instance writer-enum :enum "BAZ")))
    (is (typep foo writer-enum))
    (let ((foo (avro:coerce
                (avro:deserialize writer-enum (avro:serialize foo))
                reader-enum)))
      (is (typep foo reader-enum))
      (is (string= "FOO" (avro:which-one foo))))

    (is (typep bar writer-enum))
    (let ((bar (avro:coerce
                (avro:deserialize writer-enum (avro:serialize bar))
                reader-enum)))
      (is (typep bar reader-enum))
      (is (string= "BAR" (avro:which-one bar))))

    (is (typep baz writer-enum))
    (let ((baz (avro:coerce
                (avro:deserialize writer-enum (avro:serialize baz))
                reader-enum)))
      (is (typep baz reader-enum))
      (is (string= "BAR" (avro:which-one baz))))))
