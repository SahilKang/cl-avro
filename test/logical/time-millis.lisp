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

(defpackage #:test/time-millis
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:json-string=))

(in-package #:test/time-millis)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "int", "logicalType": "time-millis"})
        (fingerprint #x7275d51a3f395c8f)
        (expected (find-class 'avro:time-millis)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(test io
  (let* ((expected-hour 3)
         (expected-minute 39)
         (expected-millisecond 44300)
         (object (make-instance
                  'avro:time-millis
                  :hour expected-hour
                  :minute expected-minute
                  :millisecond expected-millisecond))
         (serialized (make-array 4 :element-type '(unsigned-byte 8)
                                   :initial-contents '(#xd8 #xb4 #xc9 #xc))))
    (flet ((check (object)
             (is (= expected-hour (avro:hour object)))
             (is (= expected-minute (avro:minute object)))
             (is (= expected-millisecond (multiple-value-bind (second remainder)
                                             (avro:second object)
                                           (+ (* 1000 second)
                                              (* 1000 remainder)))))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize 'avro:time-millis serialized)))
        (is (eq (find-class 'avro:time-millis) (class-of deserialized)))
        (check deserialized)
        (is (local-time:timestamp= object deserialized))))))
