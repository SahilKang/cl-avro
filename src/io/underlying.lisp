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
(defpackage #:cl-avro.io.underlying
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:export #:to-underlying
           #:from-underlying))
(in-package #:cl-avro.io.underlying)

(defgeneric to-underlying (logical-object)
  (:documentation
   "Return the underlying representation of LOGICAL-OBJECT."))

(defgeneric from-underlying (logical-schema underlying-object)
  (:documentation
   "Make an instance of LOGICAL-SCHEMA from UNDERLYING-OBJECT."))

;;; unix time util

(declaim
 (ftype (function (local-time::timezone)
                  (values local-time:timestamp &optional))
        unix-epoch))
(defun unix-epoch (timezone)
  (local-time:encode-timestamp 0 0 0 0 1 1 1970 :timezone timezone))

;;; uuid schema

(defmethod to-underlying
    ((uuid schema:uuid))
  (the (values simple-string &optional)
       (schema:uuid uuid)))

(defmethod from-underlying
    ((schema schema:uuid-schema) (uuid simple-string))
  (declare (ignore schema))
  (make-instance 'schema:uuid :uuid uuid))

;;; date schema

(defmethod to-underlying
    ((date schema:date))
  (let* ((unix-epoch (unix-epoch local-time:+utc-zone+))
         (diff (local-time-duration:timestamp-difference date unix-epoch))
         (day-diff (local-time-duration:duration-as diff :day)))
    (the schema:int day-diff)))

(defmethod from-underlying
    ((schema schema:date-schema) (days integer))
  (declare (ignore schema)
           (schema:int days))
  (let ((timestamp
          (local-time:adjust-timestamp!
              (unix-epoch local-time:+utc-zone+)
            (offset :day days)
            (timezone local-time:+utc-zone+))))
    (change-class timestamp 'schema:date)))

;;; time-millis schema

(deftype nonnegative-int ()
  '(and (integer 0) schema:int))

(defmethod to-underlying
    ((time-millis schema:time-millis))
  (let ((hour (schema:hour time-millis))
        (minute (schema:minute time-millis)))
    (multiple-value-bind (second remainder)
        (schema:second time-millis)
      (the (values nonnegative-int &optional)
           (+ (* hour 60 60 1000)
              (* minute 60 1000)
              (* second 1000)
              (* remainder 1000))))))

(defmethod from-underlying
    ((schema schema:time-millis-schema) (milliseconds-after-midnight integer))
  (declare (ignore schema)
           (nonnegative-int milliseconds-after-midnight))
  (let* ((hour (multiple-value-bind (hour remainder)
                   (truncate milliseconds-after-midnight (* 60 60 1000))
                 (setf milliseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate milliseconds-after-midnight (* 60 1000))
                   (setf milliseconds-after-midnight remainder)
                   minute)))
    (make-instance
     'schema:time-millis
     :hour hour
     :minute minute
     :millisecond milliseconds-after-midnight)))

;;; time-micros schema

(deftype nonnegative-long ()
  '(and (integer 0) schema:long))

(defmethod to-underlying
    ((time-micros schema:time-micros))
  (let ((hour (schema:hour time-micros))
        (minute (schema:minute time-micros)))
    (multiple-value-bind (second remainder)
        (schema:second time-micros)
      (the (values nonnegative-long &optional)
           (+ (* hour 60 60 1000 1000)
              (* minute 60 1000 1000)
              (* second 1000 1000)
              (* remainder 1000 1000))))))

(defmethod from-underlying
    ((schema schema:time-micros-schema) (microseconds-after-midnight integer))
  (declare (ignore schema)
           (nonnegative-long microseconds-after-midnight))
  (let* ((hour (multiple-value-bind (hour remainder)
                   (truncate microseconds-after-midnight (* 60 60 1000 1000))
                 (setf microseconds-after-midnight remainder)
                 hour))
         (minute (multiple-value-bind (minute remainder)
                     (truncate microseconds-after-midnight (* 60 1000 1000))
                   (setf microseconds-after-midnight remainder)
                   minute)))
    (make-instance
     'schema:time-micros
     :hour hour
     :minute minute
     :microsecond microseconds-after-midnight)))

;;; timestamp-millis schema

(defmethod to-underlying
    ((timestamp-millis schema:timestamp-millis))
  (let* ((unix-epoch (unix-epoch local-time:+utc-zone+))
         (diff (local-time-duration:timestamp-difference
                timestamp-millis unix-epoch))
         (nanosecond-diff (local-time-duration:duration-as diff :nsec)))
    (the (values schema:long &optional)
         (nth-value 0 (truncate nanosecond-diff (* 1000 1000))))))

(defmethod from-underlying
    ((schema schema:timestamp-millis-schema) (milliseconds-from-unix-epoch integer))
  (declare (ignore schema)
           (schema:long milliseconds-from-unix-epoch))
  (let* ((seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate milliseconds-from-unix-epoch 1000)
             (setf milliseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* milliseconds-from-unix-epoch 1000 1000))
         (timestamp
           (local-time:adjust-timestamp!
               (unix-epoch local-time:+utc-zone+)
             (offset :sec seconds-from-unix-epoch)
             (offset :nsec nanoseconds-from-unix-epoch)
             (timezone local-time:+utc-zone+))))
    (change-class timestamp 'schema:timestamp-millis)))

;;; timestamp-micros schema

(defmethod to-underlying
    ((timestamp-micros schema:timestamp-micros))
  (let* ((unix-epoch (unix-epoch local-time:+utc-zone+))
         (diff (local-time-duration:timestamp-difference
                timestamp-micros unix-epoch))
         (nanosecond-diff (local-time-duration:duration-as diff :nsec)))
    (the (values schema:long &optional)
         (nth-value 0 (truncate nanosecond-diff 1000)))))

(defmethod from-underlying
    ((schema schema:timestamp-micros-schema) (microseconds-from-unix-epoch integer))
  (declare (ignore schema)
           (schema:long microseconds-from-unix-epoch))
  (let* ((seconds-from-unix-epoch
           (multiple-value-bind (seconds remainder)
               (truncate microseconds-from-unix-epoch (* 1000 1000))
             (setf microseconds-from-unix-epoch remainder)
             seconds))
         (nanoseconds-from-unix-epoch
           (* microseconds-from-unix-epoch 1000))
         (timestamp
           (local-time:adjust-timestamp!
               (unix-epoch local-time:+utc-zone+)
             (offset :sec seconds-from-unix-epoch)
             (offset :nsec nanoseconds-from-unix-epoch)
             (timezone local-time:+utc-zone+))))
    (change-class timestamp 'schema:timestamp-micros)))

;;; local-timestamp-millis schema

(declaim
 (ftype (function (local-time:timestamp local-time::timezone)
                  (values boolean &optional))
        daylight-savings-p))
(defun daylight-savings-p (timestamp timezone)
  (nth-value 1 (local-time:timestamp-subtimezone timestamp timezone)))

(declaim
 (ftype (function (local-time:timestamp) (values &optional))
        adjust-for-daylight-savings))
(defun adjust-for-daylight-savings (timestamp)
  (when (daylight-savings-p timestamp local-time:*default-timezone*)
    (local-time:adjust-timestamp!
        timestamp
      (offset :hour -1)))
  (values))

(declaim
 (ftype (function ((or schema:local-timestamp-millis
                       schema:local-timestamp-micros))
                  (values local-time-duration:duration &optional))
        local-diff))
(defun local-diff (timestamp)
  (let* ((timezone (schema:timezone timestamp))
         (unix-epoch (unix-epoch timezone))
         (timestamp (if (not (daylight-savings-p timestamp timezone))
                        timestamp
                        (local-time:adjust-timestamp
                            timestamp
                          (offset :hour 1)))))
    (local-time-duration:timestamp-difference timestamp unix-epoch)))

(defmethod to-underlying
    ((local-timestamp-millis schema:local-timestamp-millis))
  (let* ((diff (local-diff local-timestamp-millis))
         (nanosecond-diff (local-time-duration:duration-as diff :nsec)))
    (the (values schema:long &optional)
         (nth-value 0 (truncate nanosecond-diff (* 1000 1000))))))

(defmethod from-underlying
    ((schema schema:local-timestamp-millis-schema) (milliseconds-from-epoch integer))
  (declare (ignore schema)
           (schema:long milliseconds-from-epoch))
  (let* ((seconds-from-epoch
           (multiple-value-bind (seconds remainder)
               (truncate milliseconds-from-epoch 1000)
             (setf milliseconds-from-epoch remainder)
             seconds))
         (nanoseconds-from-epoch
           (* milliseconds-from-epoch 1000 1000))
         (timestamp
           (local-time:adjust-timestamp!
               (unix-epoch local-time:*default-timezone*)
             (offset :sec seconds-from-epoch)
             (offset :nsec nanoseconds-from-epoch))))
    (adjust-for-daylight-savings timestamp)
    (change-class timestamp 'schema:local-timestamp-millis)))

;;; local-timestamp-micros schema

(defmethod to-underlying
    ((local-timestamp-micros schema:local-timestamp-micros))
  (let* ((diff (local-diff local-timestamp-micros))
         (nanosecond-diff (local-time-duration:duration-as diff :nsec)))
    (the (values schema:long &optional)
         (nth-value 0 (truncate nanosecond-diff 1000)))))

(defmethod from-underlying
    ((schema schema:local-timestamp-micros-schema) (microseconds-from-epoch integer))
  (declare (ignore schema)
           (schema:long microseconds-from-epoch))
  (let* ((seconds-from-epoch
           (multiple-value-bind (seconds remainder)
               (truncate microseconds-from-epoch (* 1000 1000))
             (setf microseconds-from-epoch remainder)
             seconds))
         (nanoseconds-from-epoch
           (* microseconds-from-epoch 1000))
         (timestamp
           (local-time:adjust-timestamp!
               (unix-epoch local-time:*default-timezone*)
             (offset :sec seconds-from-epoch)
             (offset :nsec nanoseconds-from-epoch))))
    (adjust-for-daylight-savings timestamp)0
    (change-class timestamp 'schema:local-timestamp-micros)))
