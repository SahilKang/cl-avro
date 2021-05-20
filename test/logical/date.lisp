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

(defpackage #:test/date
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:json-string=))

(in-package #:test/date)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "int", "logicalType": "date"})
        (fingerprint #x7275d51a3f395c8f)
        (expected (find-class 'avro:date)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(test io
  (let* ((expected-year 2021)
         (expected-month 5)
         (expected-day 5)
         (object (make-instance
                  'avro:date
                  :year expected-year
                  :month expected-month
                  :day expected-day))
         (serialized (make-array 3 :element-type '(unsigned-byte 8)
                                   :initial-contents '(#x80 #xa5 #x02))))
    (flet ((check (object)
             (is (= expected-year (avro:year object)))
             (is (= expected-month (avro:month object)))
             (is (= expected-day (avro:day object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize 'avro:date serialized)))
        (is (eq (find-class 'avro:date) (class-of deserialized)))
        (check deserialized)
        (is (local-time:timestamp= object deserialized))))))
