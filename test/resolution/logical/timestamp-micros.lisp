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
(defpackage #:test/resolution/timestamp-micros
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:microsecond))
(in-package #:test/resolution/timestamp-micros)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp) (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:year writer) (avro:year reader)))
  (is (= (avro:month writer) (avro:month reader)))
  (is (= (avro:day writer) (avro:day reader)))
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (microsecond writer) (microsecond reader)))
  (values))

(test timestamp-micros->timestamp-micros
  (let* ((writer (make-instance
                  'avro:timestamp-micros
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :microsecond 32350450))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:timestamp-micros (avro:serialize writer))
                  'avro:timestamp-micros)))
    (is (typep writer 'avro:timestamp-micros))
    (is (typep reader 'avro:timestamp-micros))
    (assert= writer reader)))

(test timestamp-millis->timestamp-micros
  (let* ((writer (make-instance
                  'avro:timestamp-millis
                  :year 2021
                  :month 8
                  :day 7
                  :hour 2
                  :minute 25
                  :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize
                   'avro:timestamp-millis (avro:serialize writer))
                  'avro:timestamp-micros)))
    (is (typep writer 'avro:timestamp-millis))
    (is (typep reader 'avro:timestamp-micros))
    (assert= writer reader)))
