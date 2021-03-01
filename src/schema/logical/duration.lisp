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
(defpackage #:cl-avro.schema.logical.duration
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.complex
                #:complex-schema
                #:ensure-superclass
                #:fixed
                #:size)
  (:export #:duration
           #:underlying
           #:duration-object
           #:months
           #:days
           #:milliseconds))
(in-package #:cl-avro.schema.logical.duration)

(defclass duration-object ()
  ((months
    :initarg :months
    :reader months
    :type (unsigned-byte 32)
    :documentation "Number of months.")
   (days
    :initarg :days
    :reader days
    :type (unsigned-byte 32)
    :documentation "Number of days.")
   (milliseconds
    :initarg :milliseconds
    :reader milliseconds
    :type (unsigned-byte 32)
    :documentation "Number of milliseconds."))
  (:metaclass complex-schema)
  (:default-initargs
   :months 0
   :days 0
   :milliseconds 0)
  (:documentation
   "Base class for objects adhering to an avro duration schema."))

(defmethod initialize-instance :after
    ((instance duration-object) &key)
  (with-slots (months days milliseconds) instance
    (check-type months (unsigned-byte 32))
    (check-type days (unsigned-byte 32))
    (check-type milliseconds (unsigned-byte 32))))

(defclass duration (logical-schema)
  ((underlying
    :type fixed))
  (:documentation
   "Base class for avro duration schemas."))

(defmethod closer-mop:validate-superclass
    ((class duration) (superclass logical-schema))
  t)

(defmethod initialize-instance :around
    ((instance duration) &rest initargs)
  (ensure-superclass duration-object)
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance duration) &key)
  (let* ((underlying (underlying instance))
         (size (size underlying)))
    (unless (= size 12)
      (error "Size of fixed schema must be 12, not ~S" size))))
