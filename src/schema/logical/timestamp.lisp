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
(defpackage #:cl-avro.schema.logical.timestamp
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
                #:hour
                #:minute)
  (:shadowing-import-from #:cl-avro.schema.logical.time
                          #:second)
  (:export #:timestamp-millis-schema
           #:timestamp-micros-schema
           #:underlying

           #:timestamp-millis
           #:timestamp-micros

           #:year
           #:month
           #:day
           #:hour
           #:minute
           #:second))
(in-package #:cl-avro.schema.logical.timestamp)

;;; timestamp-millis

(defclass timestamp-millis-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro timestamp-millis schema."))

(defclass timestamp-millis (local-time:timestamp)
  ()
  (:metaclass timestamp-millis-schema)
  (:documentation
   "Avro timestamp-millis.

This represents a millisecond-precision instant on the global
timeline, independent of a particular timezone or calendar."))

(defparameter +min-timestamp-millis+
  (let* ((milliseconds-from-unix-epoch
           (- (expt 2 63)))
         (seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate milliseconds-from-unix-epoch 1000)
             (setf milliseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* milliseconds-from-unix-epoch 1000 1000)))
    (local-time:adjust-timestamp!
        (local-time:encode-timestamp
         0 0 0 0 1 1 1970 :timezone local-time:+utc-zone+)
      (offset :sec seconds-from-unix-epoch)
      (offset :nsec nanoseconds-from-unix-epoch))))

(defparameter +max-timestamp-millis+
  (let* ((milliseconds-from-unix-epoch
           (1- (expt 2 63)))
         (seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate milliseconds-from-unix-epoch 1000)
             (setf milliseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* milliseconds-from-unix-epoch 1000 1000)))
    (local-time:adjust-timestamp!
        (local-time:encode-timestamp
         0 0 0 0 1 1 1970 :timezone local-time:+utc-zone+)
      (offset :sec seconds-from-unix-epoch)
      (offset :nsec nanoseconds-from-unix-epoch))))

(defmethod initialize-instance :after
    ((instance timestamp-millis)
     &key year month day hour minute millisecond
       (timezone local-time:*default-timezone*))
  (when (or year month hour minute millisecond timezone)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour day month year
       :timezone timezone :into instance)))
  (unless (and (local-time:timestamp>= instance +min-timestamp-millis+)
               (local-time:timestamp<= instance +max-timestamp-millis+))
    (error "Timestamp out of bounds")))

(defmethod year
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-year instance :timezone timezone))

(defmethod month
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-month instance :timezone timezone))

(defmethod day
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-day instance :timezone timezone))

(defmethod hour
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-hour instance :timezone timezone))

(defmethod minute
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-minute instance :timezone timezone))

(defmethod second
    ((instance timestamp-millis)
     &key (timezone local-time:*default-timezone*))
  (let ((second (local-time:timestamp-second instance :timezone timezone))
        (millisecond (local-time:timestamp-millisecond instance)))
    (values second (/ millisecond 1000))))

;;; timestamp-micros

(defclass timestamp-micros-schema (logical-schema)
  ((underlying
    :type (eql long)))
  (:default-initargs
   :underlying 'long)
  (:documentation
   "Avro timestamp-micros schema."))

(defclass timestamp-micros (local-time:timestamp)
  ()
  (:metaclass timestamp-micros-schema)
  (:documentation
   "Avro timestamp-micros.

This represents a microsecond-precision instant on the global
timeline, independent of a particular timezone or calendar."))

(defparameter +min-timestamp-micros+
  (let* ((microseconds-from-unix-epoch
           (- (expt 2 63)))
         (seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate microseconds-from-unix-epoch (* 1000 1000))
             (setf microseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* microseconds-from-unix-epoch 1000)))
    (local-time:adjust-timestamp!
        (local-time:encode-timestamp
         0 0 0 0 1 1 1970 :timezone local-time:+utc-zone+)
      (offset :sec seconds-from-unix-epoch)
      (offset :nsec nanoseconds-from-unix-epoch))))

(defparameter +max-timestamp-micros+
  (let* ((microseconds-from-unix-epoch
           (1- (expt 2 63)))
         (seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate microseconds-from-unix-epoch (* 1000 1000))
             (setf microseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* microseconds-from-unix-epoch 1000)))
    (local-time:adjust-timestamp!
        (local-time:encode-timestamp
         0 0 0 0 1 1 1970 :timezone local-time:+utc-zone+)
      (offset :sec seconds-from-unix-epoch)
      (offset :nsec nanoseconds-from-unix-epoch))))

(defmethod initialize-instance :after
    ((instance timestamp-micros)
     &key year month day hour minute microsecond
       (timezone local-time:*default-timezone*))
  (when (or year month hour minute microsecond timezone)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour day month year
       :timezone timezone :into instance)))
  (unless (and (local-time:timestamp>= instance +min-timestamp-micros+)
               (local-time:timestamp<= instance +max-timestamp-micros+))
    (error "Timestamp out of bounds")))

(defmethod year
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-year instance :timezone timezone))

(defmethod month
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-month instance :timezone timezone))

(defmethod day
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-day instance :timezone timezone))

(defmethod hour
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-hour instance :timezone timezone))

(defmethod minute
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (local-time:timestamp-minute instance :timezone timezone))

(defmethod second
    ((instance timestamp-micros)
     &key (timezone local-time:*default-timezone*))
  (let ((second (local-time:timestamp-second instance :timezone timezone))
        (microsecond (local-time:timestamp-microsecond instance)))
    (values second (/ microsecond (* 1000 1000)))))
