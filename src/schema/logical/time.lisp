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
(defpackage #:cl-avro.schema.logical.time
  (:use #:cl)
  (:shadow #:second)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.primitive
                #:int
                #:long)
  (:export #:time-millis-schema
           #:time-micros-schema
           #:underlying

           #:time-millis-mixin
           #:time-micros-mixin
           #:time-millis
           #:time-micros

           #:hour
           #:minute
           #:second))
(in-package #:cl-avro.schema.logical.time)

;;; mixins

(defclass hour-minute-mixin (local-time:timestamp)
  ()
  (:documentation
   "hour-minute mixin."))

(defgeneric hour (time-of-day)
  (:documentation "Return hour.")
  (:method ((instance hour-minute-mixin))
    (local-time:timestamp-hour instance)))

(defgeneric minute (time-of-day)
  (:documentation "Return minute.")
  (:method ((instance hour-minute-mixin))
    (local-time:timestamp-minute instance)))

(defgeneric second (time-of-day)
  (:documentation "Return (values second remainder)."))

(defclass time-millis-mixin (hour-minute-mixin)
  ()
  (:documentation
   "time-millis mixin."))

(defmethod second ((instance time-millis-mixin))
  (let ((second (local-time:timestamp-second instance))
        (millisecond (local-time:timestamp-millisecond instance)))
    (values second (/ millisecond 1000))))

(defclass time-micros-mixin (hour-minute-mixin)
  ()
  (:documentation
   "time-micros mixin."))

(defmethod second ((instance time-micros-mixin))
  (let ((second (local-time:timestamp-second instance))
        (microsecond (local-time:timestamp-microsecond instance)))
    (values second (/ microsecond (* 1000 1000)))))

;;; time-millis

(defclass time-millis-schema (logical-schema)
  ((underlying
    :type (eql int)))
  (:default-initargs
   :underlying 'int)
  (:documentation
   "Avro time-millis schema."))

(defmethod closer-mop:validate-superclass
    ((class time-millis-schema) (superclass logical-schema))
  t)

(defclass time-millis (time-millis-mixin)
  ()
  (:metaclass time-millis-schema)
  (:documentation
   "Avro time-millis.

This represents a millisecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod initialize-instance :after
    ((instance time-millis) &key hour minute millisecond)
  (when (or hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour 1 1 1 :into instance))))

;;; time-micros

(defclass time-micros-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro time-micros schema."))

(defmethod closer-mop:validate-superclass
    ((class time-micros-schema) (superclass logical-schema))
  t)

(defclass time-micros (time-micros-mixin)
  ()
  (:metaclass time-micros-schema)
  (:documentation
   "Avro time-micros.

This represents a microsecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod initialize-instance :after
    ((instance time-micros) &key hour minute microsecond)
  (when (or hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour 1 1 1 :into instance))))
