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
(defpackage #:test/resolution/local-timestamp-millis
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:millisecond))
(in-package #:test/resolution/local-timestamp-millis)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp) (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:year writer) (avro:year reader)))
  (is (= (avro:month writer) (avro:month reader)))
  (is (= (avro:day writer) (avro:day reader)))
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (millisecond writer) (millisecond reader)))
  (values))

(test local-timestamp-millis->local-timestamp-millis
  (let* ((writer (make-instance
                  'avro:local-timestamp-millis
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:local-timestamp-millis (avro:serialize writer))
                  'avro:local-timestamp-millis)))
    (is (typep writer 'avro:local-timestamp-millis))
    (is (typep reader 'avro:local-timestamp-millis))
    (assert= writer reader)))

(test local-timestamp-millis->long
  (let* ((writer (make-instance
                  'avro:local-timestamp-millis
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:local-timestamp-millis (avro:serialize writer))
                  'avro:long)))
    (is (typep writer 'avro:local-timestamp-millis))
    (is (typep reader 'avro:long))
    (is (= 1628303132350 reader))))

(test long->local-timestamp-millis
  (let* ((writer 1628303132350)
         (reader (avro:coerce
                  (avro:deserialize 'avro:long (avro:serialize writer))
                  'avro:local-timestamp-millis)))
    (is (typep writer 'avro:long))
    (is (typep reader 'avro:local-timestamp-millis))
    (is (= 2021 (avro:year reader)))
    (is (= 8 (avro:month reader)))
    (is (= 7 (avro:day reader)))
    (is (= 2 (avro:hour reader)))
    (is (= 25 (avro:minute reader)))
    (multiple-value-bind (second remainder)
        (avro:second reader)
      (is (= 32 second))
      (is (= (/ 350 1000) remainder)))))

(test local-timestamp-millis->float
  (let* ((writer (make-instance
                  'avro:local-timestamp-millis
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:local-timestamp-millis (avro:serialize writer))
                  'avro:float)))
    (is (typep writer 'avro:local-timestamp-millis))
    (is (typep reader 'avro:float))
    (is (= 1628303132350.0 reader))))

(test int->local-timestamp-millis
  (let* ((writer 8732350)
         (reader (avro:coerce
                  (avro:deserialize 'avro:int (avro:serialize writer))
                  'avro:local-timestamp-millis)))
    (is (typep writer 'avro:int))
    (is (typep reader 'avro:local-timestamp-millis))
    (is (= 1970 (avro:year reader)))
    (is (= 1 (avro:month reader)))
    (is (= 1 (avro:day reader)))
    (is (= 2 (avro:hour reader)))
    (is (= 25 (avro:minute reader)))
    (multiple-value-bind (second remainder)
        (avro:second reader)
      (is (= 32 second))
      (is (= (/ 350 1000) remainder)))))

(test local-timestamp-micros->local-timestamp-millis
  (let* ((writer (make-instance
                  'avro:local-timestamp-micros
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :microsecond 32350450))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:local-timestamp-micros (avro:serialize writer))
                  'avro:local-timestamp-millis)))
    (is (typep writer 'avro:local-timestamp-micros))
    (is (typep reader 'avro:local-timestamp-millis))
    (assert= writer reader)))
