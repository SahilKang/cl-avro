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
(defpackage #:cl-avro/test/local-timestamp-micros
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))
(in-package #:cl-avro/test/local-timestamp-micros)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "local-timestamp-micros"})
        (fingerprint #x10d2958f23b25beb)
        (expected (find-class 'avro:local-timestamp-micros)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint expected)))))

(define-io-test io
    ((year 2021)
     (month 5)
     (day 14)
     (hour 2)
     (minute 54)
     (microsecond 23800900))
    avro:local-timestamp-micros
    (make-instance
     'avro:local-timestamp-micros
     :year year
     :month month
     :day day
     :hour hour
     :minute minute
     :microsecond microsecond)
    (#x88 #xb9 #xda #xc7 #xab #x90 #xe1 #x05)
  (is (local-time:timestamp= object arg))
  (is (= year (avro:year arg)))
  (is (= month (avro:month arg)))
  (is (= day (avro:day arg)))
  (is (= hour (avro:hour arg)))
  (is (= minute (avro:minute arg)))
  (is (= microsecond
         (multiple-value-bind (second remainder)
             (avro:second arg)
           (+ (* 1000 1000 second)
              (* 1000 1000 remainder))))))
