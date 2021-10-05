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
(defpackage #:test/resolution/date
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros))
(in-package #:test/resolution/date)

(test date->date
  (let* ((writer (make-instance 'avro:date :year 2021 :month 8 :day 6))
         (reader (avro:coerce
                  (avro:deserialize 'avro:date (avro:serialize writer))
                  'avro:date)))
    (is (typep writer 'avro:date))
    (is (typep reader 'avro:date))
    (is (= (avro:year writer) (avro:year reader)))
    (is (= (avro:month writer) (avro:month reader)))
    (is (= (avro:day writer) (avro:day reader)))))
