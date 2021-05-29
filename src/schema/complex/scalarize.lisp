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
(defpackage #:cl-avro.schema.complex.scalarize
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex.common
                #:define-initializers)
  (:import-from #:cl-avro.schema.complex.base
                #:ensure-superclass)
  (:export #:scalarize-class
           #:scalarize-value))
(in-package #:cl-avro.schema.complex.scalarize)

;;; scalarize-class

(defclass scalarize-class (standard-class)
  ((scalarize
    :initarg :scalarize
    :reader scalarize
    :type list
    :documentation "Initargs to scalarize."))
  (:default-initargs
   :scalarize nil))

(defmethod closer-mop:validate-superclass
    ((class scalarize-class) (superclass standard-class))
  t)

(define-initializers scalarize-class :around
    (&rest initargs)
  (ensure-superclass scalarize-object)
  (apply #'call-next-method instance initargs))

(define-initializers scalarize-class :after
    (&key)
  (flet ((assert-keyword (arg)
           (check-type arg keyword)))
    (map nil #'assert-keyword (scalarize instance))))

;;; scalarize-object

(defclass scalarize-object ()
  ())

(declaim (ftype (function (cons) (values t &optional)) scalarize-list))
(defun scalarize-list (list)
  (if (= (length list) 1)
      (first list)
      list))

(declaim (ftype (function (t) (values t &optional)) scalarize-value))
(defun scalarize-value (value)
  (if (consp value)
      (scalarize-list value)
      value))

(declaim
 (ftype (function (scalarize-class) (values list &optional))
        initargs-to-scalarize))
(defun initargs-to-scalarize (class)
  (loop
    for superclass in (closer-mop:class-direct-superclasses class)

    when (typep superclass 'scalarize-class)
      append (scalarize superclass) into initargs

    finally
       (return
         (delete-duplicates
          (append (scalarize class) initargs)))))

(define-initializers scalarize-object :around
    (&rest initargs)
  (loop
    with initargs-to-scalarize = (initargs-to-scalarize (class-of instance))

    for remaining = initargs then (cddr remaining)
    while remaining
    for arg = (car remaining)
    for rest = (cdr remaining)
    for value = (car rest)

    unless rest do
      (error "Odd number of key-value pairs: ~S" initargs)

    if (member arg initargs-to-scalarize)
      nconc (list arg (scalarize-value value)) into scalarized-initargs
    else
      nconc (list arg value) into scalarized-initargs

    finally
       (return
         (apply #'call-next-method instance scalarized-initargs))))
