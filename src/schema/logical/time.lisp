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

           #:time-millis-base
           #:time-micros-base
           #:time-millis
           #:time-micros

           #:hour
           #:minute
           #:second))
(in-package #:cl-avro.schema.logical.time)

;;; base classes

(defclass time-of-day (local-time:timestamp)
  ()
  (:documentation
   "Base class for avro time-of-day classes."))

(defmethod initialize-instance :after
    ((instance time-of-day) &key hour minute)
  (when (or hour minute)
    (local-time:adjust-timestamp! instance
      (set :hour hour)
      (set :minute minute))))

(defclass time-millis-base (time-of-day)
  ()
  (:documentation
   "Base class for avro time millis classes."))

(defmethod initialize-instance :after
    ((instance time-millis-base) &key hour minute millisecond)
  (when (or hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:adjust-timestamp! instance
        (set :sec second)
        (set :nsec (* remainder 1000 1000))))))

(defclass time-micros-base (time-of-day)
  ()
  (:documentation
   "Base class for avro time micros classes."))

(defmethod initialize-instance :after
    ((instance time-micros-base) &key hour minute microsecond)
  (when (or hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:adjust-timestamp! instance
        (set :sec second)
        (set :nsec (* remainder 1000))))))

(defgeneric hour (time-of-day)
  (:documentation "Return hour.")
  (:method ((instance time-of-day))
    (local-time:timestamp-hour instance)))

(defgeneric minute (time-of-day)
  (:documentation "Return minute.")
  (:method ((instance time-of-day))
    (local-time:timestamp-minute instance)))

(defgeneric second (time-of-day)
  (:documentation "Return (values second remainder).")

  (:method ((instance time-millis-base))
    (let ((second (local-time:timestamp-second instance))
          (millisecond (local-time:timestamp-millisecond instance)))
      (values second (/ millisecond 1000))))

  (:method ((instance time-micros-base))
    (let ((second (local-time:timestamp-second instance))
          (microsecond (local-time:timestamp-microsecond instance)))
      (values second (/ microsecond (* 1000 1000))))))

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

(defclass time-millis (time-millis-base)
  ()
  (:metaclass time-millis-schema)
  (:documentation
   "Avro time-millis.

This represents a millisecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod initialize-instance :after
    ((instance time-millis) &key hour minute millisecond)
  (when (or hour minute millisecond)
    (setf (local-time:day-of instance) 0))
  (check-type instance local-time:time-of-day))

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

(defclass time-micros (time-micros-base)
  ()
  (:metaclass time-micros-schema)
  (:documentation
   "Avro time-micros.

This represents a microsecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod initialize-instance :after
    ((instance time-micros) &key hour minute microsecond)
  (when (or hour minute microsecond)
    (setf (local-time:day-of instance) 0))
  (check-type instance local-time:time-of-day))
