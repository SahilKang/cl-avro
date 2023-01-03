;;; Copyright 2021-2023 Google LLC
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
(defpackage #:cl-avro.internal.name.class
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:mop #:cl-avro.internal.mop)
   (#:type #:cl-avro.internal.name.type)
   (#:deduce #:cl-avro.internal.name.deduce)
   (#:intern #:cl-avro.internal.intern))
  (:export #:named-class
           #:provided-name
           #:deduced-name
           #:provided-namespace
           #:deduced-namespace
           #:fullname))
(in-package #:cl-avro.internal.name.class)

(defclass named-class (standard-class)
  ((provided-name
    :reader provided-name
    :type type:fullname
    :documentation "Provided class name.")
   (provided-namespace
    :initarg :namespace
    :type type:namespace
    :documentation "Provided class namespace.")
   (deduced-name
    :reader deduced-name
    :type type:name
    :documentation "Namespace unqualified name of class.")
   (deduced-namespace
    :reader deduced-namespace
    :type type:namespace
    :documentation "Namespace of class.")
   (fullname
    :reader api:fullname
    :type type:fullname
    :documentation "Namespace qualified name of class."))
  (:metaclass mop:scalar-class)
  (:scalars :name :namespace)
  (:documentation
   "Base class for named metaclasses."))

(defmethod closer-mop:validate-superclass
    ((class named-class) (superclass standard-class))
  t)

;;; name

(deftype api:name-return-type ()
  "(values deduced provided)"
  '(values type:name type:fullname &optional))

(defmethod api:name
    ((instance named-class))
  "Returns (values deduced provided).

The returned values conform to NAME-RETURN-TYPE."
  (let ((deduced-name (deduced-name instance))
        (provided-name (provided-name instance)))
    (declare (type:name deduced-name)
             (type:fullname provided-name))
    (values deduced-name provided-name)))

;;; namespace

(declaim
 (ftype (function (named-class) (values boolean &optional))
        namespace-provided-p))
(defun namespace-provided-p (named-class)
  "True if namespace was provided."
  (slot-boundp named-class 'provided-namespace))

(declaim
 (ftype (function (named-class) (values type:namespace &optional))
        provided-namespace))
(defun provided-namespace (named-class)
  "Returns the provided-namespace."
  (when (namespace-provided-p named-class)
    (slot-value named-class 'provided-namespace)))

(deftype api:namespace-return-type ()
  "(values deduced provided provided-p)"
  '(values type:namespace type:namespace boolean))

(defmethod api:namespace
    ((instance named-class))
  "Returns (values deduced provided provided-p).

The returned values conform to NAMESPACE-RETURN-TYPE."
  (let ((deduced-namespace (deduced-namespace instance))
        (provided-namespace (provided-namespace instance))
        (namespace-provided-p (namespace-provided-p instance)))
    (declare (type:namespace deduced-namespace provided-namespace)
             (boolean namespace-provided-p))
    (values deduced-namespace provided-namespace namespace-provided-p)))

;;; initialization

(declaim (ftype (function (list) (values type:fullname &optional)) parse-name))
(defun parse-name (initargs)
  (let* ((first (member :name initargs))
         (second (member :name (cddr first))))
    (assert first () "Must supply NAME")
    (if second
        (second second)
        (string (second first)))))

(defmethod initialize-instance :after
    ((instance named-class) &rest initargs)
  (let ((provided-namespace (provided-namespace instance)))
    (with-slots
          (provided-name deduced-name deduced-namespace fullname) instance
      (setf provided-name (parse-name initargs)
            deduced-name (deduce:fullname->name provided-name)
            deduced-namespace (deduce:deduce-namespace
                               provided-name provided-namespace nil)
            fullname (deduce:deduce-fullname
                      provided-name provided-namespace nil)))))

(defmethod reinitialize-instance :around
    ((instance named-class) &rest initargs)
  (when (typep (getf initargs :name) '(or null (not symbol)))
    (push (class-name instance) initargs)
    (push :name initargs))
  (apply #'call-next-method instance initargs))

;;; intern

(defmethod api:intern :around
    ((instance named-class) &key (null-namespace api:*null-namespace*))
  (let ((name (api:name instance))
        (namespace (api:namespace instance)))
    (when (deduce:null-namespace-p namespace)
      (setf namespace null-namespace))
    (let* ((intern:*intern-package* (or (find-package namespace)
                                        (make-package namespace)))
           (class-name (intern name intern:*intern-package*)))
      (assert (not (find-class class-name nil)) (class-name) "Class already exists")
      (export class-name intern:*intern-package*)
      (setf (find-class class-name) instance)
      (call-next-method instance :null-namespace null-namespace)
      class-name)))
