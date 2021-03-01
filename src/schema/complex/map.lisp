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
(defpackage #:cl-avro.schema.complex.map
  (:use #:cl)
  (:shadow #:map
           #:values)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema)
  (:export #:map
           #:map-object
           #:values))
(in-package #:cl-avro.schema.complex.map)

(defclass map-object ()
  ((map
    :initarg :map
    :reader map
    :type hash-table
    :documentation "Hash-table mapping strings to values."))
  (:metaclass complex-schema)
  (:default-initargs
   :map (error "Must supply MAP"))
  (:documentation
   "Base class for objects adhering to an avro map schema."))

(defclass map (complex-schema)
  ((values
    :initarg :values
    :reader values
    :type schema
    :documentation "Map schema value type."))
  (:default-initargs
   :values (error "Must supply VALUES"))
  (:documentation
   "Base class for avro map schemas."))

(defmethod closer-mop:validate-superclass
    ((class map) (superclass complex-schema))
  t)

(declaim
 (ftype (function (t t schema) (cl:values &optional)) assert-keyval)
 (inline assert-keyval))
(defun assert-keyval (key value schema)
  (declare (optimize (speed 3) (safety 0)))
  (check-type key simple-string)
  (unless (typep value schema)
    (error "Expected type ~S, but got ~S for ~S"
           schema (type-of value) value))
  (cl:values))
(declaim (notinline assert-keyval))

(declaim
 (ftype (function (hash-table schema) (cl:values hash-table &optional))
        parse-hash-table)
 (inline parse-hash-table))
(defun parse-hash-table (map schema)
  (declare (optimize (speed 3) (safety 0))
           (inline assert-keyval))
  (let ((test (hash-table-test map)))
    (unless (eq test 'equal)
      (error "Expected test to be #'equal, not ~S" test)))
  (flet ((assert-keyval (key value)
           (assert-keyval key value schema)))
    (maphash #'assert-keyval map))
  map)
(declaim (notinline parse-hash-table))

(declaim
 (ftype (function (cons schema) (cl:values hash-table &optional)) parse-alist)
 (inline parse-alist))
(defun parse-alist (map schema)
  (declare (optimize (speed 3) (safety 0))
           (inline assert-keyval))
  (loop
    with hash-table = (make-hash-table :test #'equal)

    for (k . v) in map
    unless (nth-value 1 (gethash k hash-table)) do
      (assert-keyval k v schema)
      (setf (gethash k hash-table) v)

    finally
       (return hash-table)))
(declaim (notinline parse-alist))

(declaim
 (ftype (function (list schema) (cl:values hash-table &optional)) parse-plist)
 (inline parse-plist))
(defun parse-plist (map schema)
  (declare (optimize (speed 3) (safety 0))
           (inline assert-keyval))
  (loop
    with hash-table = (make-hash-table :test #'equal)

    for remaining = map then (cddr remaining)
    while remaining
    for k = (car remaining)
    for rest = (cdr remaining)
    for v = (car rest)

    unless rest do
      (error "Odd number of key-value pairs: ~S" map)
    unless (nth-value 1 (gethash k hash-table)) do
      (assert-keyval k v schema)
      (setf (gethash k hash-table) v)

    finally
       (return hash-table)))
(declaim (notinline parse-plist))

(declaim
 (ftype (function (list schema) (cl:values hash-table &optional)) parse-list)
 (inline parse-list))
(defun parse-list (map schema)
  (declare (optimize (speed 3) (safety 0))
           (inline parse-alist parse-plist))
  ;; as is normal with alist/plists, when there are duplicate keys,
  ;; only the first value is taken
  (if (consp (first map))
      (parse-alist map schema)
      (parse-plist map schema)))
(declaim (notinline parse-list))

(declaim
 (ftype (function (t schema) (cl:values hash-table &optional)) parse-map)
 (inline parse-map))
(defun parse-map (map schema)
  (declare (optimize (speed 3) (safety 0))
           (inline parse-hash-table parse-list))
  (etypecase map
    (hash-table (parse-hash-table map schema))
    (list (parse-list map schema))))
(declaim (notinline parse-map))

(defmethod initialize-instance :around
    ((instance map-object) &rest initargs &key (map nil map-p))
  (declare (optimize (speed 3) (safety 0))
           (inline parse-map))
  (if map-p
      (let* ((schema (values (class-of instance)))
             (map (parse-map map schema)))
        (setf (getf initargs :map) map)
        (apply #'call-next-method instance initargs))
      (call-next-method)))

(defmethod initialize-instance :around
    ((instance map) &rest initargs)
  (ensure-superclass map-object)
  (apply #'call-next-method instance initargs))
