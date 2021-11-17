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
(defpackage #:cl-avro/test/resolution/union
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)))
(in-package #:cl-avro/test/resolution/union)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<double> ()
  ()
  (:metaclass avro:array)
  (:items avro:double))

(defclass writer-schema ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null avro:string avro:int array<int>))

(defclass reader-schema ()
  ()
  (:metaclass avro:union)
  (:schemas avro:string avro:float avro:long array<double>))

(test both-unions
  (let ((null (make-instance 'writer-schema :object nil))
        (string (make-instance 'writer-schema :object "foobar"))
        (int (make-instance 'writer-schema :object 2))
        (array (make-instance
                'writer-schema
                :object (make-instance 'array<int> :initial-contents '(2 4 6)))))
    (is (typep null 'writer-schema))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer-schema (avro:serialize null))
       'reader-schema))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize string))
                   'reader-schema)))
      (is (typep string 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 0 (nth-value 1 (avro:which-one reader))))
      (is (string= (avro:object string) (avro:object reader))))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize int))
                   'reader-schema)))
      (is (typep int 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 1 (nth-value 1 (avro:which-one reader))))
      (is (= (avro:object int) (avro:object reader))))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize array))
                   'reader-schema)))
      (is (typep array 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 3 (nth-value 1 (avro:which-one reader))))
      (is (every #'= (avro:object array) (avro:object reader))))))

(test reader-union
  (let ((reader (avro:coerce
                 (avro:deserialize 'avro:int (avro:serialize 3))
                 'reader-schema)))
    (is (typep reader 'reader-schema))
    (is (= 1 (nth-value 1 (avro:which-one reader))))
    (is (= 3 (avro:object reader))))

  (signals error
    (avro:coerce
     (avro:deserialize 'avro:null (avro:serialize nil))
     'reader-schema)))

(test writer-union
  (let ((reader (avro:coerce
                 (avro:deserialize
                  'writer-schema
                  (avro:serialize (make-instance 'writer-schema :object 3)))
                 'avro:long)))
    (is (typep reader 'avro:long))
    (is (= 3 reader)))

  (signals error
    (avro:coerce
     (avro:deserialize
      'writer-schema
      (avro:serialize (make-instance 'writer-schema :object 3)))
     'avro:string)))
