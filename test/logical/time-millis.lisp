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
                #:json-string=
                #:define-io-test))

(in-package #:test/time-millis)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "int", "logicalType": "time-millis"})
        (fingerprint #x7275d51a3f395c8f)
        (expected (find-class 'avro:time-millis)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint64 expected)))))

(define-io-test io
    ((hour 3)
     (minute 39)
     (millisecond 44300))
    avro:time-millis
    (make-instance
     'avro:time-millis
     :hour hour
     :minute minute
     :millisecond millisecond)
    (#xd8 #xb4 #xc9 #xc)
  (is (local-time:timestamp= object arg))
  (is (= hour (avro:hour arg)))
  (is (= minute (avro:minute arg)))
  (is (= millisecond (multiple-value-bind (second remainder)
                         (avro:second arg)
                       (+ (* 1000 second)
                          (* 1000 remainder))))))
