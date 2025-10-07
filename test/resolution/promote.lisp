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
(defpackage #:cl-avro/test/resolution/promote
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/resolution/base
                #:find-schema
                #:bytes))
(in-package #:cl-avro/test/resolution/promote)

(defmacro deftest (from to input function &optional postprocess)
  (declare (symbol from to function postprocess))
  (let* ((from (find-schema from))
         (to (find-schema to))
         (test-name (intern (format nil "~A->~A" from to)))
         (input-object (gensym))
         (serialized (gensym))
         (deserialized (gensym))
         (postprocessed (if postprocess
                            `(,postprocess ,input-object)
                            input-object)))
    `(test ,test-name
       (let* ((,input-object ,input)
              (,serialized (avro:serialize ,input-object))
              (,deserialized (avro:coerce
                              (avro:deserialize ',from ,serialized)
                              ',to)))
         (is (typep ,input-object ',from))
         (is (typep ,deserialized ',to))
         (is (,function ,postprocessed ,deserialized))))))

(deftest int long 3 =)

(deftest int float 4 =)

(deftest int double 5 =)

(deftest long float 6 =)

(deftest long double 7 =)

(deftest float double 8.7 =)

(deftest string bytes "foobar" equalp babel:string-to-octets)

(deftest bytes string (bytes #x61 #x62 #x63) string= babel:octets-to-string)
