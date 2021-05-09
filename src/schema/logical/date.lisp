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
(defpackage #:cl-avro.schema.logical.date
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.primitive
                #:int)
  (:export #:date-schema
           #:underlying
           #:date-mixin
           #:date
           #:year
           #:month
           #:day))
(in-package #:cl-avro.schema.logical.date)

;;; mixin

(defclass date-mixin (local-time:timestamp)
  ()
  (:documentation
   "Date mixin."))

(defgeneric year (date)
  (:documentation "Return year.")
  (:method ((instance date-mixin))
    (local-time:timestamp-year instance)))

(defgeneric month (date)
  (:documentation "Return month.")
  (:method ((instance date-mixin))
    (local-time:timestamp-month instance)))

(defgeneric day (date)
  (:documentation "Return day.")
  (:method ((instance date-mixin))
    (local-time:timestamp-day instance)))

;;; date

(defclass date-schema (logical-schema)
  ((underlying
    :type (eql int)))
  (:default-initargs
   :underlying 'int)
  (:documentation
   "Avro date schema."))

(defmethod closer-mop:validate-superclass
    ((class date-schema) (superclass logical-schema))
  t)

(defclass date (date-mixin)
  ()
  (:metaclass date-schema)
  (:documentation
   "Avro date.

This represents a date on the calendar, with no reference to a
particular timezone or time-of-day."))

(defmethod initialize-instance :after
    ((instance date) &key year month day)
  (when (or year month)
    (local-time:encode-timestamp 0 0 0 0 day month year :into instance)))
