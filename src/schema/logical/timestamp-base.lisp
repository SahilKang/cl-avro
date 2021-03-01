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
(defpackage #:cl-avro.schema.logical.timestamp-base
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.date
                #:date-mixin
                #:year
                #:month
                #:day)
  (:import-from #:cl-avro.schema.logical.time
                #:time-millis-mixin
                #:time-micros-mixin
                #:hour
                #:minute)
  (:shadowing-import-from #:cl-avro.schema.logical.time
                          #:second)
  (:export #:timestamp-millis-base
           #:timestamp-micros-base
           #:year
           #:month
           #:day
           #:hour
           #:minute
           #:second))
(in-package #:cl-avro.schema.logical.timestamp-base)

(defclass timestamp-millis-base (date-mixin time-millis-mixin)
  ())

(defmethod initialize-instance :after
    ((instance timestamp-millis-base)
     &key year month day hour minute millisecond)
  (when (or year month hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour day month year :into instance))))

(defclass timestamp-micros-base (date-mixin time-micros-mixin)
  ())

(defmethod initialize-instance :after
    ((instance timestamp-micros-base)
     &key year month day hour minute microsecond)
  (when (or year month hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour day month year :into instance))))
