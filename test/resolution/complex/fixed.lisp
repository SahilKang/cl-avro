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
(defpackage #:test/resolution/fixed
  (:use #:cl #:1am))
(in-package #:test/resolution/fixed)

(test fixed
  (let* ((writer-schema (make-instance
                         'avro:fixed
                         :name "foo.bar"
                         :size 3))
         (reader-schema (make-instance
                         'avro:fixed
                         :name "baz"
                         :aliases '("baz.bar")
                         :size 3))
         (writer-object (make-instance writer-schema :initial-contents '(2 4 6)))
         (reader-object (avro:deserialize
                         writer-schema
                         (avro:serialize writer-object)
                         :reader-schema reader-schema)))
    (is (typep writer-object writer-schema))
    (is (typep reader-object reader-schema))
    (is (equal (coerce writer-object 'list) (coerce reader-object 'list)))))
