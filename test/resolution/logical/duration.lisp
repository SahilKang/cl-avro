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
(defpackage #:test/resolution/duration
  (:use #:cl #:1am))
(in-package #:test/resolution/duration)

(defclass writer_fixed ()
  ()
  (:metaclass avro:fixed)
  (:size 12))

(defclass reader_fixed ()
  ()
  (:metaclass avro:fixed)
  (:size 12))

(defclass writer-schema ()
  ()
  (:metaclass avro:duration)
  (:underlying writer_fixed))

(defclass reader-schema ()
  ()
  (:metaclass avro:duration)
  (:underlying reader_fixed))

(test duration->duration
  (let* ((writer (make-instance 'writer-schema :months 3 :days 4 :milliseconds 5))
         (reader (avro:deserialize
                  'writer-schema
                  (avro:serialize writer)
                  :reader-schema 'reader-schema)))
    (is (typep writer 'writer-schema))
    (is (typep reader 'reader-schema))
    (is (= (avro:months writer) (avro:months reader)))
    (is (= (avro:days writer) (avro:days reader)))
    (is (= (avro:milliseconds writer) (avro:milliseconds reader)))))
