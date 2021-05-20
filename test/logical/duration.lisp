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

(defpackage #:test/duration
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:define-schema-test
                #:json-syntax))

(in-package #:test/duration)

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

(test io
  (let* ((schema (make-instance
                  'avro:duration
                  :underlying (make-instance
                               'avro:fixed
                               :name "foo"
                               :size 12)))
         (expected-months 39)
         (expected-days 17)
         (expected-milliseconds 14831053)
         (object (make-instance
                  schema
                  :years 3
                  :months 3
                  :weeks 2
                  :days 3
                  :hours 4
                  :minutes 7
                  :seconds 11
                  :milliseconds 3
                  :nanoseconds 50000000))
         (serialized
           (make-array
            12
            :element-type '(unsigned-byte 8)
            :initial-contents '(#x27 #x00 #x00 #x00
                                #x11 #x00 #x00 #x00
                                #xcd #x4d #xe2 #x00))))
    (flet ((check (object)
             (is (= expected-months (avro:months object)))
             (is (= expected-days (avro:days object)))
             (is (= expected-milliseconds (avro:milliseconds object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      ;; TODO check with size 11 to make sure fixed is returned
      ;; do this with the other logical-fallthrough tests
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (class-of deserialized)))
        (check deserialized)))))
