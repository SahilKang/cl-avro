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

(defpackage #:test/timestamp-micros
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))

(in-package #:test/timestamp-micros)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "timestamp-micros"})
        (fingerprint #xd054e14493f41db7)
        (expected (find-class 'avro:timestamp-micros)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(define-io-test io
    ((year 2021)
     (month 5)
     (day 14)
     (hour 0)
     (minute 52)
     (microsecond 17500300))
    avro:timestamp-micros
    (make-instance
     'avro:timestamp-micros
     :year year
     :month month
     :day day
     :hour hour
     :minute minute
     :microsecond microsecond
     :timezone local-time:+utc-zone+)
    (#x98 #xd2 #xe6 #xfc #xf4 #x8f #xe1 #x05)
  (is (local-time:timestamp= object arg))
  (is (= year (avro:year arg)))
  (is (= month (avro:month arg)))
  (is (= day (avro:day arg :timezone local-time:+utc-zone+)))
  (is (= hour (avro:hour arg :timezone local-time:+utc-zone+)))
  (is (= minute (avro:minute arg :timezone local-time:+utc-zone+)))
  (is (= microsecond
         (multiple-value-bind (second remainder)
             (avro:second arg :timezone local-time:+utc-zone+)
           (+ (* 1000 1000 second)
              (* 1000 1000 remainder))))))
