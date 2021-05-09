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

(defpackage #:cl-avro.schema.logical
  (:use #:cl-avro.schema.logical.base
        #:cl-avro.schema.logical.uuid
        #:cl-avro.schema.logical.date
        #:cl-avro.schema.logical.time
        #:cl-avro.schema.logical.timestamp
        #:cl-avro.schema.logical.local-timestamp
        #:cl-avro.schema.logical.decimal
        #:cl-avro.schema.logical.duration)
  (:export #:logical-schema
           #:underlying

           #:uuid-schema
           #:uuid

           #:date-schema
           #:date
           #:year
           #:month
           #:day

           #:time-millis-schema
           #:time-micros-schema
           #:time-millis
           #:time-micros
           #:hour
           #:minute
           #:second

           #:timestamp-millis-schema
           #:timestamp-micros-schema
           #:timestamp-millis
           #:timestamp-micros

           #:local-timestamp-millis-schema
           #:local-timestamp-micros-schema
           #:local-timestamp-millis
           #:local-timestamp-micros

           #:decimal
           #:decimal-object
           #:unscaled
           #:scale
           #:precision

           #:duration
           #:duration-object
           #:months
           #:days
           #:milliseconds))
