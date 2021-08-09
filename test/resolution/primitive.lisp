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
(defpackage #:test/resolution/primitive
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:find-schema
                #:bytes))
(in-package #:test/resolution/primitive)

(defmacro deftest (schema function expected)
  (declare (symbol schema function))
  (let ((schema (find-schema schema))
        (test-name (intern (format nil "~A->~:*~A" schema)))
        (expected-object (gensym))
        (serialized (gensym))
        (deserialized (gensym)))
    `(test ,test-name
       (let* ((,expected-object ,expected)
              (,serialized (avro:serialize ,expected-object))
              (,deserialized (avro:deserialize
                              ',schema ,serialized :reader-schema ',schema)))
         (is (typep ,expected-object ',schema))
         (is (,function ,expected-object ,deserialized))))))

(deftest null eq nil)

(deftest boolean eq 'avro:true)

(deftest int = 3)

(deftest long = 4)

(deftest float = 3.7)

(deftest double = 3.7d0)

(deftest bytes equalp (bytes 2 4 6))

(deftest string string= "foobar")
