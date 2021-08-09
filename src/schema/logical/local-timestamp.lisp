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
(defpackage #:cl-avro.schema.logical.local-timestamp
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.primitive
                #:long)
  (:import-from #:cl-avro.schema.logical.date
                #:year
                #:month
                #:day)
  (:import-from #:cl-avro.schema.logical.time
                #:time-millis-mixin
                #:time-micros-mixin
                #:hour
                #:minute)
  (:import-from #:cl-avro.schema.logical.timezone
                #:timezone-mixin
                #:timezone)
  (:import-from #:cl-avro.schema.complex
                #:scalarize-class)
  (:shadowing-import-from #:cl-avro.schema.logical.time
                          #:second)
  (:export #:local-timestamp-millis-schema
           #:local-timestamp-micros-schema
           #:underlying

           #:local-timestamp-millis
           #:local-timestamp-micros

           #:year
           #:month
           #:day
           #:hour
           #:minute
           #:second
           #:timezone))
(in-package #:cl-avro.schema.logical.local-timestamp)

;;; date-mixin

(defclass date-mixin (local-time:timestamp timezone-mixin)
  ()
  (:documentation
   "Date mixin."))

(defmethod year
    ((instance date-mixin) &key)
  (local-time:timestamp-year instance :timezone (timezone instance)))

(defmethod month
    ((instance date-mixin) &key)
  (local-time:timestamp-month instance :timezone (timezone instance)))

(defmethod day
    ((instance date-mixin) &key)
  (local-time:timestamp-day instance :timezone (timezone instance)))

;;; local-timestamp-millis

(defclass local-timestamp-millis-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:metaclass scalarize-class)
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro local-timestamp-millis schema."))

(defclass local-timestamp-millis (date-mixin time-millis-mixin)
  ()
  (:metaclass local-timestamp-millis-schema)
  (:documentation
   "Avro local-timestamp-millis.

This represents a millisecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))

(defmethod initialize-instance :after
    ((instance local-timestamp-millis)
     &key year month day hour minute millisecond)
  (when (or year month hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour day month year
       :timezone (timezone instance) :into instance))))

;;; local-timestamp-micros

(defclass local-timestamp-micros-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:metaclass scalarize-class)
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro local-timestamp-micros schema."))

(defclass local-timestamp-micros (date-mixin time-micros-mixin)
  ()
  (:metaclass local-timestamp-micros-schema)
  (:documentation
   "Avro local-timestamp-micros.

This represents a microsecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))

(defmethod initialize-instance :after
    ((instance local-timestamp-micros)
     &key year month day hour minute microsecond)
  (when (or year month hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour day month year
       :timezone (timezone instance) :into instance))))
