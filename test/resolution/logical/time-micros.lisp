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
(defpackage #:test/resolution/time-micros
  (:use #:cl #:1am)
  (:import-from #:test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros
                #:microsecond))
(in-package #:test/resolution/time-micros)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp) (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (microsecond writer) (microsecond reader)))
  (values))

(test time-micros->time-micros
  (let* ((writer (make-instance 'avro:time-micros :hour 2 :minute 25 :microsecond 32350450))
         (reader (avro:deserialize
                  'avro:time-micros
                  (avro:serialize writer)
                  :reader-schema 'avro:time-micros)))
    (is (typep writer 'avro:time-micros))
    (is (typep reader 'avro:time-micros))
    (assert= writer reader)))

(test time-micros->long
  (let* ((writer (make-instance 'avro:time-micros :hour 2 :minute 25 :microsecond 32350450))
         (reader (avro:deserialize
                  'avro:time-micros
                  (avro:serialize writer)
                  :reader-schema 'avro:long)))
    (is (typep writer 'avro:time-micros))
    (is (typep reader 'avro:long))
    (is (= 8732350450 reader))))

(test long->time-micros
  (let* ((writer 8732350450)
         (reader (avro:deserialize
                  'avro:long
                  (avro:serialize writer)
                  :reader-schema 'avro:time-micros)))
    (is (typep writer 'avro:long))
    (is (typep reader 'avro:time-micros))
    (is (= 2 (avro:hour reader)))
    (is (= 25 (avro:minute reader)))
    (multiple-value-bind (second remainder)
        (avro:second reader)
      (is (= 32 second))
      (is (= (/ 350450 1000 1000) remainder)))))

(test time-micros->float
  (let* ((writer (make-instance 'avro:time-micros :hour 2 :minute 25 :microsecond 32350450))
         (reader (avro:deserialize
                  'avro:time-micros
                  (avro:serialize writer)
                  :reader-schema 'avro:float)))
    (is (typep writer 'avro:time-micros))
    (is (typep reader 'avro:float))
    (is (= 8732350450.0 reader))))

(test int->time-micros
  (let* ((writer 8732350)
         (reader (avro:deserialize
                  'avro:int
                  (avro:serialize writer)
                  :reader-schema 'avro:time-micros)))
    (is (typep writer 'avro:int))
    (is (typep reader 'avro:time-micros))
    (is (= 0 (avro:hour reader)))
    (is (= 0 (avro:minute reader)))
    (multiple-value-bind (second remainder)
        (avro:second reader)
      (is (= 8 second))
      (is (= (/ 732350 1000 1000) remainder)))))

(test time-millis->time-micros
  (let* ((writer (make-instance 'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:deserialize
                  'avro:time-millis
                  (avro:serialize writer)
                  :reader-schema 'avro:time-micros)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:time-micros))
    (assert= writer reader)))

(defmacro timestamp->time-micros (from)
  (declare (symbol from))
  (let ((test-name (intern (format nil "~A->TIME-MICROS" from)))
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
                       :reader-schema 'avro:time-micros)))
         (is (typep writer ',from))
         (is (typep reader 'avro:time-micros))
         (assert= writer reader)))))

(timestamp->time-micros timestamp-millis)

(timestamp->time-micros timestamp-micros)

(timestamp->time-micros local-timestamp-millis)

(timestamp->time-micros local-timestamp-micros)
