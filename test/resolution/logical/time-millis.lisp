;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro/test/resolution/time-millis
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros
                #:millisecond))
(in-package #:cl-avro/test/resolution/time-millis)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp)
                  (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (millisecond writer) (millisecond reader)))
  (values))

(test time-millis->time-millis
  (let* ((writer (make-instance
                  'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize 'avro:time-millis (avro:serialize writer))
                  'avro:time-millis)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:time-millis))
    (assert= writer reader)))
