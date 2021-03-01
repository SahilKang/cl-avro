;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/logical
  (:use #:cl #:1am))

(in-package #:test/logical)

(test decimal-bytes
  (flet ((make-json (precision &optional scale)
           (format nil
                   "{type: \"bytes\",
                     logicalType: \"decimal\",
                     precision: ~A~@[,
                     scale: ~A~]}"
                   precision
                   scale)))
    (let* ((expected-precision 3)
           (expected-scale 2)
           (json (make-json expected-precision expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= expected-scale (avro:scale schema)))
      (let ((value 200))
        (is (= value (avro:unscaled
                      (avro:deserialize
                       schema
                       (avro:serialize
                        (make-instance schema :unscaled value)))))))
      (signals error
        (make-instance schema :unscaled 2000)))

    (let* ((expected-precision 3)
           (expected-scale expected-precision)
           (json (make-json expected-precision expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= expected-scale (avro:scale schema)))
      (let ((value 200))
        (is (= value (avro:unscaled
                      (avro:deserialize
                       schema
                       (avro:serialize
                        (make-instance schema :unscaled value)))))))
      (signals error
        (make-instance schema :unscaled 2000)))

    (let* ((expected-precision 3)
           (expected-scale (1+ expected-precision))
           (json (make-json expected-precision expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (eq 'avro:bytes schema)))

    (let* ((expected-precision 3)
           (json (make-json expected-precision))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= 0 (avro:scale schema))))))

(test decimal-fixed
  (flet ((make-json (precision size &optional scale)
           (format nil
                   "{type: {type: \"fixed\", name: \"foo\", size: ~A},
                     logicalType: \"decimal\",
                     precision: ~A~@[,
                     scale: ~A~]}"
                   size
                   precision
                   scale)))
    (let* ((expected-precision 3)
           (expected-scale 2)
           (expected-size 2)
           (json (make-json expected-precision expected-size expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= expected-scale (avro:scale schema)))
      (is (= expected-size (avro:size (avro:underlying schema))))
      (let ((value 200))
        (is (= value (avro:unscaled
                      (avro:deserialize
                       schema
                       (avro:serialize
                        (make-instance schema :unscaled value)))))))
      (signals error
        (make-instance schema :unscaled 2000)))

    (let* ((expected-precision 3)
           (expected-scale expected-precision)
           (expected-size 2)
           (json (make-json expected-precision expected-size expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= expected-scale (avro:scale schema)))
      (is (= expected-size (avro:size (avro:underlying schema))))
      (let ((value 200))
        (is (= value (avro:unscaled
                      (avro:deserialize
                       schema
                       (avro:serialize
                        (make-instance schema :unscaled value)))))))
      (signals error
        (make-instance schema :unscaled 2000)))

    (let* ((expected-precision 3)
           (expected-scale (1+ expected-precision))
           (expected-size 2)
           (json (make-json expected-precision expected-size expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (typep schema 'avro:fixed))
      (is (= expected-size (avro:size schema))))

    (let* ((expected-precision 3)
           (expected-scale 2)
           (expected-size 1)
           (json (make-json expected-precision expected-size expected-scale))
           (schema (avro:deserialize 'avro:schema json)))
      (is (typep schema 'avro:fixed))
      (is (= expected-size (avro:size schema))))

    (let* ((expected-precision 3)
           (expected-size 4)
           (json (make-json expected-precision expected-size))
           (schema (avro:deserialize 'avro:schema json)))
      (is (= expected-precision (avro:precision schema)))
      (is (= 0 (avro:scale schema)))
      (is (= expected-size (avro:size (avro:underlying schema)))))))


(test uuid
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"string\", logicalType: \"uuid\"}"))
        (expected "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
    (is (eq (find-class 'avro:uuid) schema))
    (is (string= expected (avro:uuid
                           (avro:deserialize
                            schema
                            (avro:serialize
                             (make-instance schema :uuid expected))))))
    (signals error
      (make-instance schema :uuid "abc"))
    (signals error
      (make-instance schema :uuid ""))
    (signals (or error warning)
      (make-instance schema :uuid 123))
    (signals (or error warning)
      (make-instance schema :uuid nil)))
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"bytes\", logicalType: \"uuid\"}")))
    (is (eq 'avro:bytes schema))))


(test date
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"int\", logicalType: \"date\"}"))
        (expected '(1 2 3)))
    (is (eq (find-class 'avro:date) schema))
    (is (equal expected
               (let ((date (avro:deserialize
                            schema
                            (avro:serialize
                             (make-instance
                              schema
                              :year (first expected)
                              :month (second expected)
                              :day (third expected))))))
                 (list (avro:year date)
                       (avro:month date)
                       (avro:day date)))))
    (signals error
      (make-instance schema :year "abc")))
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"string\", logicalType: \"date\"}")))
    (is (eq 'avro:string schema))))


(test time-millis
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"int\", logicalType: \"time-millis\"}"))
        (expected '(1 2 3)))
    (is (eq (find-class 'avro:time-millis) schema))
    (is (equal expected
               (let ((time (avro:deserialize
                            schema
                            (avro:serialize
                             (make-instance
                              schema
                              :hour (first expected)
                              :minute (second expected)
                              :millisecond (third expected))))))
                 (list (avro:hour time)
                       (avro:minute time)
                       (multiple-value-bind (second remainder)
                           (avro:second time)
                         (+ (* 1000 second)
                            (* 1000 remainder)))))))
    (signals error
      (make-instance schema :hour -1)))
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"bytes\", logicalType: \"time-millis\"}")))
    (is (eq 'avro:bytes schema))))


(test time-micros
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"long\", logicalType: \"time-micros\"}"))
        (expected '(1 2 3)))
    (is (eq (find-class 'avro:time-micros) schema))
    (is (equal expected
               (let ((time (avro:deserialize
                            schema
                            (avro:serialize
                             (make-instance
                              schema
                              :hour (first expected)
                              :minute (second expected)
                              :microsecond (third expected))))))
                 (list (avro:hour time)
                       (avro:minute time)
                       (multiple-value-bind (second remainder)
                           (avro:second time)
                         (+ (* 1000 1000 second)
                            (* 1000 1000 remainder)))))))
    (signals error
      (make-instance schema :hour -1)))
  (let ((schema (avro:deserialize
                 'avro:schema"{type: \"bytes\", logicalType: \"time-micros\"}")))
    (is (eq 'avro:bytes schema))))


(test timestamp-millis
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"long\", logicalType: \"timestamp-millis\"}"))
        (expected '(1 2 3 4 5 6)))
    (is (eq (find-class 'avro:timestamp-millis) schema))
    (is (equal expected
               (let ((timestamp (avro:deserialize
                                 schema
                                 (avro:serialize
                                  (make-instance
                                   schema
                                   :year (first expected)
                                   :month (second expected)
                                   :day (third expected)
                                   :hour (fourth expected)
                                   :minute (fifth expected)
                                   :millisecond (sixth expected))))))
                 (list (avro:year timestamp)
                       (avro:month timestamp)
                       (avro:day timestamp)
                       (avro:hour timestamp)
                       (avro:minute timestamp)
                       (multiple-value-bind (second remainder)
                           (avro:second timestamp)
                         (+ (* 1000 second)
                            (* 1000 remainder)))))))
    (signals error
      (make-instance schema :year "abc")))
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"bytes\", logicalType: \"timestamp-millis\"}")))
    (is (eq 'avro:bytes schema))))


(test timestamp-micros
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"long\", logicalType: \"timestamp-micros\"}"))
        (expected (list 1 2 3 4 5 6)))
    (is (eq (find-class 'avro:timestamp-micros) schema))
    (is (equal expected
               (let ((timestamp (avro:deserialize
                                 schema
                                 (avro:serialize
                                  (make-instance
                                   schema
                                   :year (first expected)
                                   :month (second expected)
                                   :day (third expected)
                                   :hour (fourth expected)
                                   :minute (fifth expected)
                                   :microsecond (sixth expected))))))
                 (list (avro:year timestamp)
                       (avro:month timestamp)
                       (avro:day timestamp)
                       (avro:hour timestamp)
                       (avro:minute timestamp)
                       (multiple-value-bind (second remainder)
                           (avro:second timestamp)
                         (+ (* 1000 1000 second)
                            (* 1000 1000 remainder)))))))
    (signals error
      (make-instance schema :year "abc")))
  (let ((schema (avro:deserialize
                 'avro:schema "{type: \"bytes\", logicalType: \"timestamp-micros\"}")))
    (is (eq 'avro:bytes schema))))


(test duration
  (flet ((make-json (size)
           (format nil
                   "{type: {type: \"fixed\", name: \"foo\", size: ~A},
                     logicalType: \"duration\"}"
                   size)))
    (let* ((size 12)
           (schema (avro:deserialize 'avro:schema (make-json size)))
           (expected '(2 4 6)))
      (is (typep schema 'avro:duration))
      (is (= size (avro:size (avro:underlying schema))))
      (is (equal expected
                 (let ((duration (avro:deserialize
                                  schema
                                  (avro:serialize
                                   (make-instance
                                    schema
                                    :months (first expected)
                                    :days (second expected)
                                    :milliseconds (third expected))))))
                   (list (avro:months duration)
                         (avro:days duration)
                         (avro:milliseconds duration)))))
      (signals (or error warning)
        (make-instance schema :months 2 :days 4 :milliseconds (expt 2 32))))

    (let* ((size 11)
           (schema (avro:deserialize 'avro:schema (make-json size))))
      (is (typep schema 'avro:fixed))
      (is (= size (avro:size schema))))))
