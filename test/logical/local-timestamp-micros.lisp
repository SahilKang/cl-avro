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

(defpackage #:test/local-timestamp-micros
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:json-string=))

(in-package #:test/local-timestamp-micros)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "local-timestamp-micros"})
        (fingerprint #xd054e14493f41db7)
        (expected (find-class 'avro:local-timestamp-micros)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(test io
  (let* ((expected-year 2021)
         (expected-month 5)
         (expected-day 14)
         (expected-hour 2)
         (expected-minute 54)
         (expected-microsecond 23800900)
         (object (make-instance
                  'avro:local-timestamp-micros
                  :year expected-year
                  :month expected-month
                  :day expected-day
                  :hour expected-hour
                  :minute expected-minute
                  :microsecond expected-microsecond))
         (serialized
           (make-array
            8
            :element-type '(unsigned-byte 8)
            :initial-contents '(#x88 #xb9 #xda #xc7 #xab #x90 #xe1 #x05))))
    (flet ((check (object)
             (is (= expected-year (avro:year object)))
             (is (= expected-month (avro:month object)))
             (is (= expected-day (avro:day object)))
             (is (= expected-hour (avro:hour object)))
             (is (= expected-minute (avro:minute object)))
             (is (= expected-microsecond
                    (multiple-value-bind (second remainder)
                        (avro:second object)
                      (+ (* 1000 1000 second)
                         (* 1000 1000 remainder)))))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized
              (avro:deserialize 'avro:local-timestamp-micros serialized)))
        (is (eq (find-class 'avro:local-timestamp-micros)
                (class-of deserialized)))
        (check deserialized)
        (is (local-time:timestamp= object deserialized))))))
