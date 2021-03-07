;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/compare
  (:use #:cl #:1am))

(in-package #:test/compare)

(defmacro with-compare ((json &optional initarg) &body body)
  (declare (string json)
           ((or null keyword) initarg))
  "Evaluate BODY under a context with a COMPARE function.

COMPARE is a binary function that calls AVRO:COMPARE on a schema
created from JSON and its two serialized arguments."
  (let ((schema (gensym)))
    `(let ((,schema (avro:deserialize 'avro:schema ,json)))
       (flet ((compare (left right)
                (avro:compare ,schema
                              (avro:serialize
                               ,(if initarg
                                    `(make-instance ,schema ,initarg left)
                                    'left))
                              (avro:serialize
                               ,(if initarg
                                    `(make-instance ,schema ,initarg right)
                                    'right)))))
         ,@body))))


(test null-compare
  (with-compare ("\"null\"")
    (is (= 0 (compare nil nil)))))

(test boolean-compare
  (with-compare ("\"boolean\"")
    (is (= 0 (compare 'avro:true 'avro:true)))
    (is (= -1 (compare 'avro:false 'avro:true)))
    (is (= 1 (compare 'avro:true 'avro:false)))
    (is (= 0 (compare 'avro:false 'avro:false)))))

(test int-compare
  (with-compare ("\"int\"")
    (is (= 0 (compare 2 2)))
    (is (= -1 (compare -1 0)))
    (is (= 1 (compare 2 1)))))

(test long-compare
  (with-compare ("\"long\"")
    (is (= 0 (compare 20 20)))
    (is (= -1 (compare 2 4)))
    (is (= 1 (compare 4 2)))))

(test float-compare
  (with-compare ("\"float\"")
    (is (= 0 (compare 2.3f0 2.3f0)))
    (is (= -1 (compare 2.1f0 2.2f0)))
    (is (= 1 (compare 2.2f0 2.1f0)))))

(test double-compare
  (with-compare ("\"double\"")
    (is (= 0 (compare 2.3d0 2.3d0)))
    (is (= -1 (compare 2.1d0 2.2d0)))
    (is (= 1 (compare 2.2d0 2.1d0)))))

(test bytes-compare
  (with-compare ("\"bytes\"")
    (flet ((bytes (&rest bytes)
             (make-array (length bytes) :element-type '(unsigned-byte 8)
                                        :initial-contents bytes)))
      (is (= 0 (compare (bytes 2 4 6) (bytes 2 4 6))))
      (is (= -1 (compare (bytes 2 4) (bytes 2 4 6))))
      (is (= -1 (compare (bytes 2 4 5) (bytes 2 4 6))))
      (is (= 1 (compare (bytes 2 4 6) (bytes 2 4 5))))
      (is (= 1 (compare (bytes 2 4 6) (bytes 2 4)))))))

(test string-compare
  (with-compare ("\"string\"")
    (is (= 0 (compare "abc" "abc")))
    (is (= -1 (compare "abc" "abd")))
    (is (= -1 (compare "ab" "abc")))
    (is (= 1 (compare "ਸਾਹਿਲ ਕੰਗ" "abd")))
    (is (= 1 (compare "abc" "ab")))))

(test fixed-compare
  (with-compare ("{type: \"fixed\", name: \"foo\", size: 3}" :bytes)
    (flet ((bytes (&rest bytes)
             (make-array (length bytes) :element-type '(unsigned-byte 8)
                                        :initial-contents bytes)))
      (is (= 0 (compare (bytes 2 4 6) (bytes 2 4 6))))
      (is (= -1 (compare (bytes 2 4 5) (bytes 2 4 6))))
      (is (= 1 (compare (bytes 2 4 6) (bytes 2 4 5)))))))

(test array-compare
  (let* ((enum (make-instance
                'avro:enum
                :name "foo"
                :symbols '("ABC" "AB")))
         (array (make-instance 'avro:array :items enum)))
    (labels
        ((string->enum (string)
           (make-instance enum :enum string))
         (enums (&rest enums)
           (avro:serialize
            (map array #'string->enum enums)))
         (compare (left right)
           (let ((left (apply #'enums left))
                 (right (apply #'enums right)))
             (avro:compare array left right))))
      (is (= 0 (compare '("ABC" "ABC") '("ABC" "ABC"))))
      (is (= -1 (compare '("ABC" "ABC") '("ABC" "AB"))))
      (is (= -1 (compare '("ABC") '("ABC" "ABC"))))
      (is (= 1 (compare '("ABC" "AB") '("ABC" "ABC"))))
      (is (= 1 (compare '("AB" "AB") '("AB")))))))

(test enum-compare
  (with-compare
      ("{type: \"enum\",
         name: \"foo\",
         symbols: [\"ABC\", \"AB\"]}"
       :enum)
    (is (= 0 (compare "AB" "AB")))
    (is (= -1 (compare "ABC" "AB")))
    (is (= 1 (compare "AB" "ABC")))))

(test union-compare
  (let* ((enum (make-instance
                'avro:enum
                :name "foo"
                :symbols '("ABC" "AB")))
         (union (make-instance
                 'avro:union
                 :schemas (list 'avro:int enum))))
    (labels
        ((to-union-object (object)
           (make-instance
            union
            :object (if (stringp object)
                        (make-instance enum :enum object)
                        object)))
         (compare (left right)
           (let ((left (avro:serialize (to-union-object left)))
                 (right (avro:serialize (to-union-object right))))
             (avro:compare union left right))))
      (is (= 0 (compare 2 2)))
      (is (= 0 (compare "AB" "AB")))
      (is (= -1 (compare 2 3)))
      (is (= -1 (compare "ABC" "AB")))
      (is (= -1 (compare 2 "ABC")))
      (is (= 1 (compare 3 2)))
      (is (= 1 (compare "AB" "ABC")))
      (is (= 1 (compare "ABC" 2))))))

(test record-compare
  (let* ((enum (or (find-class 'enum_name nil)
                   ;; TODO can just use ensure-class once
                   ;; reinitialize-instance is done
                   (closer-mop:ensure-class
                    'enum_name
                    :metaclass 'avro:enum
                    :symbols '(("ABC" "AB")))))
         (map (or (find-class 'string_map nil)
                  (closer-mop:ensure-class
                   'string_map
                   :metaclass 'avro:map
                   :values '(avro:string))))
         (record (make-instance
                  'avro:record
                  :name "record_name"
                  :direct-slots
                  (list
                   (list :name '#:field_1 :type 'avro:int)
                   (list :name '#:field_2 :type enum :order 'avro:descending)
                   (list :name '#:field_3 :type map :order 'avro:ignore)))))
    (labels
        ((to-record-object (fields)
           (make-instance
            record
            :field_1 (first fields)
            :field_2 (make-instance enum :enum (second fields))
            :field_3 (make-instance map :map (third fields))))
         (compare (left right)
           (let ((left (avro:serialize (to-record-object left)))
                 (right (avro:serialize (to-record-object right))))
             (avro:compare record left right))))
      (is (= 0 (compare '(2 "ABC" ("foo" "bar")) '(2 "ABC" nil))))
      (is (= -1 (compare '(1 "ABC" ("foo" "bar")) '(2 "ABC" ("foo" "bar")))))
      (is (= -1 (compare '(2 "AB" ("foo" "bar")) '(2 "ABC" ("foo" "bar")))))
      (is (= 1 (compare '(2 "ABC" ("foo" "bar")) '(1 "ABC" ("foo" "bar")))))
      (is (= 1 (compare '(2 "ABC" ("foo" "bar")) '(2 "AB" ("foo" "bar"))))))))

(test map-compare
  (with-compare ("{type: \"map\", values: \"string\"}" :map)
    (signals error
      (compare nil nil))))
