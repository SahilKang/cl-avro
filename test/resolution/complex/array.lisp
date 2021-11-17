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
(defpackage #:cl-avro/test/resolution/array
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)
   (#:base #:cl-avro/test/resolution/base)))
(in-package #:cl-avro/test/resolution/array)

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (symbol) (values cons &optional)) find-schema))
  (defun find-schema (name)
    (handler-case
        `',(base:find-schema name)
      (error ()
        `(find-class ',name)))))

(defmacro deftest (from to input compare)
  (declare (symbol from to)
           (list input)
           ((or symbol cons) compare))
  (let ((test-name (intern (format nil "~A->~A" from to)))
        (from (find-schema from))
        (to (find-schema to))
        (writer-schema (gensym))
        (reader-schema (gensym))
        (writer-array (gensym))
        (reader-array (gensym)))
    `(test ,test-name
       (let* ((,writer-schema (make-instance 'avro:array :items ,from))
              (,reader-schema (make-instance 'avro:array :items ,to))
              (,writer-array (make-instance
                              ,writer-schema :initial-contents ,input))
              (,reader-array (avro:coerce
                              (avro:deserialize
                               ,writer-schema (avro:serialize ,writer-array))
                              ,reader-schema)))
         (is (typep ,writer-array ,writer-schema))
         (is (typep ,reader-array ,reader-schema))
         (is (= (length ,writer-array) (length ,reader-array)))
         (is (every ,compare ,writer-array ,reader-array))))))

(deftest string string '("foo" "bar") #'string=)

(deftest int long '(2 4 6) #'=)

(deftest int float '(2 4 6) #'=)

(deftest float double '(2.0 4.5 6.3) #'=)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<long> ()
  ()
  (:metaclass avro:array)
  (:items avro:long))

(deftest array<int> array<long>
  (list (make-instance 'array<int> :initial-contents '(2 4 6))
        (make-instance 'array<int> :initial-contents '(8 10 12)))
  (lambda (left right)
    (every #'= left right)))
