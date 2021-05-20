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

(defpackage #:test/timestamp-millis
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:json-string=))

(in-package #:test/timestamp-millis)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "timestamp-millis"})
        (fingerprint #xd054e14493f41db7)
        (expected (find-class 'avro:timestamp-millis)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(test io
  (let* ((expected-year 2021)
         (expected-month 5)
         (expected-day 5)
         (expected-hour 4)
         (expected-minute 4)
         (expected-millisecond 42600)
         (object (make-instance
                  'avro:timestamp-millis
                  :year expected-year
                  :month expected-month
                  :day expected-day
                  :hour expected-hour
                  :minute expected-minute
                  :millisecond expected-millisecond
                  :timezone local-time:+utc-zone+))
         (serialized
           (make-array 6 :element-type '(unsigned-byte 8)
                         :initial-contents '(#xd0 #xa7 #x98 #xab #xa7 #x5e))))
    (flet ((check (object)
             (is (= expected-year (avro:year object)))
             (is (= expected-month (avro:month object)))
             (is (= expected-day
                    (avro:day object :timezone local-time:+utc-zone+)))
             (is (= expected-hour
                    (avro:hour object :timezone local-time:+utc-zone+)))
             (is (= expected-minute
                    (avro:minute object :timezone local-time:+utc-zone+)))
             (is (= expected-millisecond
                    (multiple-value-bind (second remainder)
                        (avro:second object :timezone local-time:+utc-zone+)
                      (+ (* 1000 second)
                         (* 1000 remainder)))))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize 'avro:timestamp-millis serialized)))
        (is (eq (find-class 'avro:timestamp-millis) (class-of deserialized)))
        (check deserialized)
        (is (local-time:timestamp= object deserialized))))))
