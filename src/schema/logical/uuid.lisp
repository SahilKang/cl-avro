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
(defpackage #:cl-avro.schema.logical.uuid
  (:use #:cl)
  (:import-from #:cl-avro.schema.logical.base
                #:logical-schema
                #:underlying)
  (:import-from #:cl-avro.schema.ascii
                #:hex-p)
  (:shadowing-import-from #:cl-avro.schema.primitive
                          #:string)
  (:export #:uuid-schema
           #:underlying
           #:uuid))
(in-package #:cl-avro.schema.logical.uuid)

(declaim
 (ftype (function (simple-string) (values boolean &optional)) rfc-4122-uuid-p)
 (inline rfc-4122-uuid-p))
(defun rfc-4122-uuid-p (uuid)
  "True if UUID conforms to RFC-4122."
  (declare (optimize (speed 3) (safety 0))
           (inline hex-p))
  (and (= 36 (length uuid))
       (char= #\-
              (char uuid 8)
              (char uuid 13)
              (char uuid 18)
              (char uuid 23))
       (loop for i from 00 below 08 always (hex-p (char uuid i)))
       (loop for i from 09 below 13 always (hex-p (char uuid i)))
       (loop for i from 14 below 18 always (hex-p (char uuid i)))
       (loop for i from 19 below 23 always (hex-p (char uuid i)))
       (loop for i from 24 below 36 always (hex-p (char uuid i)))))
(declaim (notinline rfc-4122-uuid-p))

(defclass uuid-schema (logical-schema)
  ((underlying
    :type (eql string)))
  (:default-initargs
   :underlying 'string)
  (:documentation
   "Avro uuid schema."))

(defmethod closer-mop:validate-superclass
    ((class uuid-schema) (superclass logical-schema))
  t)

(defclass uuid ()
  ((uuid
    :initarg :uuid
    :reader uuid
    :type simple-string
    :documentation "UUID string conforming to RFC-4122."))
  (:metaclass uuid-schema)
  (:default-initargs
   :uuid (error "Must supply UUID"))
  (:documentation
   "Avro uuid."))

(defmethod initialize-instance :after
    ((instance uuid) &key)
  (declare (optimize (speed 3) (safety 0))
           (inline rfc-4122-uuid-p))
  (with-slots (uuid) instance
    (check-type uuid simple-string)
    (unless (rfc-4122-uuid-p uuid)
      (error "~S does not conform to RFC-4122" uuid))))
