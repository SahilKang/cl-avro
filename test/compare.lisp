;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defmacro with-compare (json &body body)
  (declare (string json))
  "Evaluate BODY under a context with a COMPARE function.

COMPARE is a binary function that calls AVRO:COMPARE on a schema
created from JSON and its two serialized arguments."
  (let ((schema (gensym)))
    `(let ((,schema (avro:json->schema ,json)))
       (flet ((compare (left right)
                (avro:compare ,schema
                              (avro:serialize nil ,schema left)
                              (avro:serialize nil ,schema right))))
         ,@body))))


(test null-compare
  (with-compare "\"null\""
    (is (= 0 (compare nil nil)))))

(test boolean-compare
  (with-compare "\"boolean\""
    (is (= 0 (compare t t)))
    (is (= -1 (compare nil t)))
    (is (= 1 (compare t nil)))
    (is (= 0 (compare nil nil)))))

(test int-compare
  (with-compare "\"int\""
    (is (= 0 (compare 2 2)))
    (is (= -1 (compare -1 0)))
    (is (= 1 (compare 2 1)))))

(test long-compare
  (with-compare "\"long\""
    (is (= 0 (compare 20 20)))
    (is (= -1 (compare 2 4)))
    (is (= 1 (compare 4 2)))))

(test float-compare
  (with-compare "\"float\""
    (is (= 0 (compare 2.3f0 2.3f0)))
    (is (= -1 (compare 2.1f0 2.2f0)))
    (is (= 1 (compare 2.2f0 2.1f0)))))

(test double-compare
  (with-compare "\"double\""
    (is (= 0 (compare 2.3d0 2.3d0)))
    (is (= -1 (compare 2.1d0 2.2d0)))
    (is (= 1 (compare 2.2d0 2.1d0)))))

(test bytes-compare
  (with-compare "\"bytes\""
    (is (= 0 (compare #(2 4 6) #(2 4 6))))
    (is (= -1 (compare #(2 4) #(2 4 6))))
    (is (= -1 (compare #(2 4 5) #(2 4 6))))
    (is (= 1 (compare #(2 4 6) #(2 4 5))))
    (is (= 1 (compare #(2 4 6) #(2 4))))))

(test string-compare
  (with-compare "\"string\""
    (is (= 0 (compare "abc" "abc")))
    (is (= -1 (compare "abc" "abd")))
    (is (= -1 (compare "ab" "abc")))
    (is (= 1 (compare "ਸਾਹਿਲ ਕੰਗ" "abd")))
    (is (= 1 (compare "abc" "ab")))))

(test fixed-compare
  (with-compare "{type: \"fixed\", name: \"foo\", size: 3}"
    (is (= 0 (compare #(2 4 6) #(2 4 6))))
    (is (= -1 (compare #(2 4 5) #(2 4 6))))
    (is (= 1 (compare #(2 4 6) #(2 4 5))))))

(test array-compare
  (with-compare "{type: \"array\",
                  items: {type: \"enum\",
                          name: \"foo\",
                          symbols: [\"ABC\", \"AB\"]}}"
    (is (= 0 (compare #("ABC" "ABC") #("ABC" "ABC"))))
    (is (= -1 (compare #("ABC" "ABC") #("ABC" "AB"))))
    (is (= -1 (compare #("ABC") #("ABC" "ABC"))))
    (is (= 1 (compare #("ABC" "AB") #("ABC" "ABC"))))
    (is (= 1 (compare #("AB" "AB") #("AB"))))))

(test enum-compare
  (with-compare "{type: \"enum\",
                  name: \"foo\",
                  symbols: [\"ABC\", \"AB\"]}"
    (is (= 0 (compare "AB" "AB")))
    (is (= -1 (compare "ABC" "AB")))
    (is (= 1 (compare "AB" "ABC")))))

(test union-compare
  (with-compare "[\"int\",
                  {type: \"enum\",
                   name: \"foo\",
                   symbols: [\"ABC\", \"AB\"]}]"
    (is (= 0 (compare 2 2)))
    (is (= 0 (compare "AB" "AB")))
    (is (= -1 (compare 2 3)))
    (is (= -1 (compare "ABC" "AB")))
    (is (= -1 (compare 2 "ABC")))
    (is (= 1 (compare 3 2)))
    (is (= 1 (compare "AB" "ABC")))
    (is (= 1 (compare "ABC" 2)))))

(test record-compare
  (with-compare "{type: \"record\",
                  name: \"record_name\",
                  fields: [
                    {name: \"field_1\",
                     type: \"int\"},
                    {name: \"field_2\",
                     type: {type: \"enum\",
                            name: \"enum_name\",
                            symbols: [\"ABC\", \"AB\"]},
                     order: \"descending\"},
                    {name: \"field_3\",
                     type: {type: \"map\", values: \"string\"},
                     order: \"ignore\"}
                  ]}"
    (let ((map-1 (make-hash-table :test #'equal))
          (map-2 (make-hash-table :test #'equal)))
      (setf (gethash "foo" map-1) "bar")

      (is (= 0 (compare (vector 2 "ABC" map-1) (vector 2 "ABC" map-2))))
      (is (= -1 (compare (vector 1 "ABC" map-1) (vector 2 "ABC" map-1))))
      (is (= -1 (compare (vector 2 "AB" map-1) (vector 2 "ABC" map-1))))
      (is (= 1 (compare (vector 2 "ABC" map-1) (vector 1 "ABC" map-1))))
      (is (= 1 (compare (vector 2 "ABC" map-1) (vector 2 "AB" map-1)))))))

(test map-compare
  (with-compare "{type: \"map\", values: \"string\"}"
    (let ((map (make-hash-table :test #'equal)))
      (signals error
        (compare map map)))))
