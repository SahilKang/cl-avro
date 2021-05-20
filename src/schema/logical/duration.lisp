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
(defpackage #:cl-avro.schema.logical.duration
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.complex
                #:complex-schema
                #:ensure-superclass
                #:fixed
                #:size
                #:define-initializers)
  (:export #:duration
           #:underlying
           #:duration-object
           #:months
           #:days
           #:milliseconds))
(in-package #:cl-avro.schema.logical.duration)

;;; schema

(defclass duration (logical-schema)
  ((underlying
    :type fixed))
  (:documentation
   "Base class for avro duration schemas."))

(defmethod closer-mop:validate-superclass
    ((class duration) (superclass logical-schema))
  t)

(define-initializers duration :around
    (&rest initargs &key underlying)
  (when (and (symbolp underlying)
             (not (typep underlying 'fixed)))
    (setf (getf initargs :underlying) (find-class underlying)))
  (ensure-superclass duration-object)
  (apply #'call-next-method instance initargs))

(define-initializers duration :after
    (&key)
  (let* ((underlying (underlying instance))
         (size (size underlying)))
    (unless (= size 12)
      (error "Size of fixed schema must be 12, not ~S" size))))

;;; object

(defclass duration-object (time-interval:time-interval)
  ((time-interval::months
    :type (unsigned-byte 32)
    :documentation "Number of months.")
   (time-interval::days
    :type (unsigned-byte 32)
    :documentation "Number of days.")
   (milliseconds
    :type (unsigned-byte 32)
    :accessor %milliseconds
    :documentation "Number of milliseconds."))
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro duration schema."))

(declaim
 (ftype (function (duration-object) (values &optional)) normalize))
(defun normalize (duration)
  (with-accessors
        ((years time-interval::interval-years)
         (months time-interval::interval-months)
         (weeks time-interval::interval-weeks)
         (days time-interval::interval-days)
         (hours time-interval::interval-hours)
         (minutes time-interval::interval-minutes)
         (seconds time-interval::interval-seconds)
         (milliseconds %milliseconds)
         (nanoseconds time-interval::interval-nanoseconds))
      duration
    (incf months (* years 12))
    (setf years 0)

    (incf days (* weeks 7))
    (setf weeks 0)

    (incf minutes (* 60 hours))
    (incf seconds (* 60 minutes))
    (setf hours 0
          minutes 0)

    (setf milliseconds (+ (* 1000 seconds)
                          (truncate nanoseconds (* 1000 1000))))

    (check-type months (unsigned-byte 32))
    (check-type days (unsigned-byte 32))
    (check-type milliseconds (unsigned-byte 32)))
  (values))

(defmethod initialize-instance :after
    ((instance duration-object) &key (milliseconds 0))
  (multiple-value-bind (seconds remainder)
      (truncate milliseconds 1000)
    (incf (time-interval::interval-seconds instance)
          seconds)
    (incf (time-interval::interval-nanoseconds instance)
          (* remainder 1000 1000)))
  (normalize instance))

(defgeneric months (duration-object)
  (:method ((instance duration-object))
    (normalize instance)
    (time-interval::interval-months instance)))

(defgeneric days (duration-object)
  (:method ((instance duration-object))
    (normalize instance)
    (time-interval::interval-days instance)))

(defgeneric milliseconds (duration-object)
  (:method ((instance duration-object))
    (normalize instance)
    (%milliseconds instance)))
