;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.complex.fixed
  (:use #:cl)
  (:import-from #:cl-avro.schema.primitive
                #:bytes)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass)
  (:import-from #:cl-avro.schema.complex.named
                #:named-schema
                #:name
                #:namespace
                #:fullname
                #:aliases)
  (:export #:fixed
           #:fixed-object
           #:size
           #:bytes
           #:name
           #:namespace
           #:fullname
           #:aliases))
(in-package #:cl-avro.schema.complex.fixed)

(defclass fixed-object ()
  ((bytes
    :initarg :bytes
    :reader bytes
    :type (simple-array (unsigned-byte 8) (*))
    :documentation "Bytes for fixed object."))
  (:metaclass complex-schema)
  (:default-initargs
   :bytes (error "Must supply BYTES"))
  (:documentation
   "Base class for objects adhering to an avro fixed schema."))

#+nil
(defmethod print-object ((object fixed-object) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream ":BYTES ~S" (bytes object))))

(defclass fixed (named-schema)
  ((size
    :initarg :size
    :reader size
    :type (integer 0)
    :documentation "Fixed schema size."))
  (:default-initargs
   :size (error "Must supply SIZE"))
  (:documentation
   "Base class for avro fixed schemas."))

(defmethod closer-mop:validate-superclass
    ((class fixed) (superclass named-schema))
  t)

(defmethod initialize-instance :around
    ((instance fixed-object) &key (bytes nil bytesp))
  (declare (optimize (speed 3) (safety 0)))
  (if bytesp
      (let* ((size (size (class-of instance)))
             (type `(simple-array (unsigned-byte 8) (,size))))
        (call-next-method instance :bytes (coerce bytes type)))
      (call-next-method)))

#+nil
(defmethod print-object ((object fixed) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream ":SIZE ~S" (size object))))

(declaim
 (ftype (function ((integer 0)) (values cons &optional)) make-bytes-slot))
(defun make-bytes-slot (size)
  (list :name 'bytes
        :type `(simple-array (unsigned-byte 8) (,size))))

(defmethod initialize-instance :around
    ((instance fixed) &rest initargs &key size)
  (let ((bytes-slot (make-bytes-slot size)))
    (push bytes-slot (getf initargs :direct-slots)))
  (ensure-superclass fixed-object)
  (apply #'call-next-method instance initargs))
