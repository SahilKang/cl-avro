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
         (reader (avro:deserialize
                  'avro:date
                  (avro:serialize writer)
                  :reader-schema 'avro:date)))
    (is (typep writer 'avro:date))
    (is (typep reader 'avro:date))
    (is (= (avro:year writer) (avro:year reader)))
    (is (= (avro:month writer) (avro:month reader)))
    (is (= (avro:day writer) (avro:day reader)))))

(test date->int
  (let* ((writer (make-instance 'avro:date :year 2021 :month 8 :day 6))
         (reader (avro:deserialize
                  'avro:date
                  (avro:serialize writer)
                  :reader-schema 'avro:int)))
    (is (typep writer 'avro:date))
    (is (typep reader 'avro:int))
    (is (= 18845 reader))))

(test int->date
  (let* ((writer 18845)
         (reader (avro:deserialize
                  'avro:int
                  (avro:serialize writer)
                  :reader-schema 'avro:date)))
    (is (typep writer 'avro:int))
    (is (typep reader 'avro:date))
    (is (= 2021 (avro:year reader)))
    (is (= 8 (avro:month reader)))
    (is (= 6 (avro:day reader)))))

(test date->float
  (let* ((writer (make-instance 'avro:date :year 2021 :month 8 :day 6))
         (reader (avro:deserialize
                  'avro:date
                  (avro:serialize writer)
                  :reader-schema 'avro:float)))
    (is (typep writer 'avro:date))
    (is (typep reader 'avro:float))
    (is (= 18845.0 reader))))

(defmacro timestamp->date (from)
  (declare (symbol from))
  (let ((test-name (intern (format nil "~A->DATE" from)))
        (from (find-schema from))
        (initarg (initarg-for-millis/micros from)))
    `(test ,test-name
       (let* ((writer (make-instance
                       ',from
                       :year 2021 :month 8 :day 6
                       :hour 1 :minute 2 ,initarg 3))
              (reader (avro:deserialize
                       ',from
                       (avro:serialize writer)
                       :reader-schema 'avro:date)))
         (is (typep writer ',from))
         (is (typep reader 'avro:date))
         (is (= (avro:year writer) (avro:year reader)))
         (is (= (avro:month writer) (avro:month reader)))
         (is (= (avro:day writer) (avro:day reader)))))))

(timestamp->date timestamp-millis)

(timestamp->date timestamp-micros)

(timestamp->date local-timestamp-millis)

(timestamp->date local-timestamp-micros)
