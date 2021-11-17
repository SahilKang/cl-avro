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
(defpackage #:cl-avro/test/time-micros
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))
(in-package #:cl-avro/test/time-micros)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "time-micros"})
        (fingerprint #xbb205318e6ed8d62)
        (expected (find-class 'avro:time-micros)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint expected)))))

(define-io-test io
    ((hour 3)
     (minute 54)
     (microsecond 17700300))
    avro:time-micros
    (make-instance
     'avro:time-micros
     :hour hour
     :minute minute
     :microsecond microsecond)
    (#x98 #xef #xbb #xde #x68)
  (is (local-time:timestamp= object arg))
  (is (= hour (avro:hour arg)))
  (is (= minute (avro:minute arg)))
  (is (= microsecond (multiple-value-bind (second remainder)
                         (avro:second arg)
                       (+ (* 1000 1000 second)
                          (* 1000 1000 remainder))))))
