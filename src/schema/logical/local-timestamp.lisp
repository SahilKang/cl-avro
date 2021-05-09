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
  (:import-from #:cl-avro.schema.logical.timestamp-base
                #:timestamp-millis-base
                #:timestamp-micros-base
                #:year
                #:month
                #:day
                #:hour
                #:minute)
  (:shadowing-import-from #:cl-avro.schema.logical.timestamp-base
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
           #:second))
(in-package #:cl-avro.schema.logical.local-timestamp)

;;; local-timestamp-millis

(defclass local-timestamp-millis-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro local-timestamp-millis schema."))

(defclass local-timestamp-millis (timestamp-millis-base)
  ()
  (:metaclass local-timestamp-millis-schema)
  (:documentation
   "Avro local-timestamp-millis.

This represents a millisecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))

;;; local-timestamp-micros

(defclass local-timestamp-micros-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro local-timestamp-micros schema."))

(defclass local-timestamp-micros (timestamp-micros-base)
  ()
  (:metaclass local-timestamp-micros-schema)
  (:documentation
   "Avro local-timestamp-micros.

This represents a microsecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))
