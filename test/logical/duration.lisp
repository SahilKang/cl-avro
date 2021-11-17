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
(defpackage #:cl-avro/test/duration
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/duration)

(named-readtables:in-readtable json-syntax)

(define-schema-test schema
  {
    "type": {
      "type": "fixed",
      "name": "foo",
      "size": 12
    },
    "logicalType": "duration"
  }
  {
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 12
    },
    "logicalType": "duration"
  }
  #x49f6bf2b9652c399
  (make-instance
   'avro:duration
   :underlying (make-instance
                'avro:fixed
                :name "foo"
                :size 12))
  (defclass |foo| ()
    ()
    (:metaclass avro:fixed)
    (:size 12))
  (defclass duration ()
    ()
    (:metaclass avro:duration)
    (:underlying |foo|)))

(define-io-test io
    ((months 39)
     (days 17)
     (milliseconds 14831053))
    (make-instance
     'avro:duration
     :underlying (make-instance 'avro:fixed :name "foo" :size 12))
    (make-instance
     schema
     :years 3
     :months 3
     :weeks 2
     :days 3
     :hours 4
     :minutes 7
     :seconds 11
     :milliseconds 3
     :nanoseconds 50000000)
    (#x27 #x00 #x00 #x00
     #x11 #x00 #x00 #x00
     #xcd #x4d #xe2 #x00)
  ;; TODO check with size 11 to make sure fixed is returned do
  ;; this with the other logical-fallthrough tests
  (is (= months (avro:months arg)))
  (is (= days (avro:days arg)))
  (is (= milliseconds (avro:milliseconds arg))))

(test late-type-check
  (setf (find-class 'late_duration) nil
        (find-class 'late_fixed) nil)

  (defclass late_duration ()
    ()
    (:metaclass avro:duration)
    (:underlying late_fixed))

  (signals error
    (internal:underlying (find-class 'late_duration)))

  (defclass late_fixed ()
    ()
    (:metaclass avro:fixed)
    (:size 12))

  (is (eq (find-class 'late_fixed) (internal:underlying (find-class 'late_duration)))))

(test bad-fixed-size
  (let ((schema (make-instance
                 'avro:duration
                 :underlying (make-instance
                              'avro:fixed
                              :name "foo"
                              :size 13))))
    (signals error
      (internal:underlying schema))))
