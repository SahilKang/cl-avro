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
(defpackage #:test/resolution/time-millis
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros
                #:millisecond))
(in-package #:test/resolution/time-millis)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp) (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (millisecond writer) (millisecond reader)))
  (values))

(test time-millis->time-millis
  (let* ((writer (make-instance 'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:deserialize
                  'avro:time-millis
                  (avro:serialize writer)
                  :reader-schema 'avro:time-millis)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:time-millis))
    (assert= writer reader)))

(test time-millis->int
  (let* ((writer (make-instance 'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:deserialize
                  'avro:time-millis
                  (avro:serialize writer)
                  :reader-schema 'avro:int)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:int))
    (is (= 8732350 reader))))

(test int->time-millis
  (let* ((writer 8732350)
         (reader (avro:deserialize
                  'avro:int
                  (avro:serialize writer)
                  :reader-schema 'avro:time-millis)))
    (is (typep writer 'avro:int))
    (is (typep reader 'avro:time-millis))
    (is (= 2 (avro:hour reader)))
    (is (= 25 (avro:minute reader)))
    (multiple-value-bind (second remainder)
        (avro:second reader)
      (is (= 32 second))
      (is (= (/ 350 1000) remainder)))))

(test time-millis->float
  (let* ((writer (make-instance 'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:deserialize
                  'avro:time-millis
                  (avro:serialize writer)
                  :reader-schema 'avro:float)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:float))
    (is (= 8732350.0 reader))))

(test time-micros->time-millis
  (let* ((writer (make-instance 'avro:time-micros :hour 2 :minute 25 :microsecond 32350450))
         (reader (avro:deserialize
                  'avro:time-micros
                  (avro:serialize writer)
                  :reader-schema 'avro:time-millis)))
    (is (typep writer 'avro:time-micros))
    (is (typep reader 'avro:time-millis))
    (assert= writer reader)))

(defmacro timestamp->time-millis (from)
  (declare (symbol from))
  (let ((test-name (intern (format nil "~A->TIME-MILLIS" from)))
        (from (find-schema from))
        (initarg (initarg-for-millis/micros from)))
    `(test ,test-name
       (let* ((writer (make-instance
                       ',from
                       :year 2021 :month 8 :day 7
                       :hour 2 :minute 25 ,initarg ,(ecase initarg
                                                      (:millisecond 32350)
                                                      (:microsecond 32350450))))
              (reader (avro:deserialize
                       ',from
                       (avro:serialize writer)
                       :reader-schema 'avro:time-millis)))
         (is (typep writer ',from))
         (is (typep reader 'avro:time-millis))
         (assert= writer reader)))))

(timestamp->time-millis timestamp-millis)

(timestamp->time-millis timestamp-micros)

(timestamp->time-millis local-timestamp-millis)

(timestamp->time-millis local-timestamp-micros)
