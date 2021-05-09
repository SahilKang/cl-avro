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
(defpackage #:cl-avro.schema.complex.named.schema
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex.common
                #:assert-distinct)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:scalarize-initargs)
  (:import-from #:cl-avro.schema.complex.named.type
                #:name
                #:namespace
                #:fullname
                #:fullname->name
                #:deduce-namespace
                #:deduce-fullname)
  (:export #:named-schema
           #:name
           #:namespace
           #:fullname
           #:aliases
           #:valid-name
           #:valid-fullname))
(in-package #:cl-avro.schema.complex.named.schema)

;;; types

(macrolet
    ((primitive-name-p (name)
       (let ((primitive-names (mapcar #'cdr +primitive->name+))
             (n (gensym)))
         `(let ((,n ,name))
            (or ,@(mapcar (lambda (schema-name)
                            `(string= ,n ,schema-name))
                          primitive-names))))))
  (declaim
   (ftype (function (t) (values boolean &optional)) not-primitive-name-p))
  (defun not-primitive-name-p (name)
    "True if NAME does not name a primitive schema in any namespace."
    (not
     (when (simple-string-p name)
       (let ((last-dot-position (position #\. name :test #'char= :from-end t)))
         (if last-dot-position
             (primitive-name-p (subseq name (1+ last-dot-position)))
             (primitive-name-p name)))))))

(deftype valid-name ()
  "A NAME which does not name a primitive schema."
  '(and name (satisfies not-primitive-name-p)))

(deftype valid-fullname ()
  "A FULLNAME which does not name a primitive schema in any namespace."
  '(and fullname (satisfies not-primitive-name-p)))

;;; named-schema

(defclass named-schema (complex-schema)
  ((provided-name
    :reader provided-name
    :type valid-fullname
    :documentation "Provided schema name.")
   (provided-namespace
    :initarg :namespace
    :type namespace
    :documentation "Provided schema namespace.")
   (deduced-name
    :reader deduced-name
    :type valid-name
    :documentation "Namespace unqualified name of schema.")
   (deduced-namespace
    :reader deduced-namespace
    :type namespace
    :documentation "Namespace of schema.")
   (fullname
    :reader fullname
    :type valid-fullname
    :documentation "Namespace qualified name of schema.")
   (aliases
    :initarg :aliases
    :reader aliases
    :type (or null (simple-array valid-fullname (*)))
    :documentation "A vector of aliases if provided, otherwise nil."))
  (:documentation
   "Base class for avro named schemas."))

(defmethod closer-mop:validate-superclass
    ((class named-schema) (superclass complex-schema))
  t)

;; aliases

(declaim
 (ftype (function
         (t boolean)
         (values (or null (simple-array valid-fullname (*))) &optional))
        parse-aliases))
(defun parse-aliases (aliases aliasesp)
  (when aliasesp
    (check-type aliases sequence)
    (let ((aliases (map '(simple-array valid-fullname (*))
                        (lambda (alias)
                          (check-type alias valid-fullname)
                          alias)
                        aliases)))
      (assert-distinct aliases)
      aliases)))

;; name

(defgeneric name (named-schema)
  (:method ((instance named-schema))
    "Returns (values deduced provided)."
    (let ((deduced-name (deduced-name instance))
          (provided-name (provided-name instance)))
      (values deduced-name provided-name))))

;; namespace

(declaim
 (ftype (function (named-schema) (values boolean &optional))
        namespace-provided-p))
(defun namespace-provided-p (named-schema)
  "True if namespace was provided."
  (slot-boundp named-schema 'provided-namespace))

(declaim
 (ftype (function (named-schema) (values namespace &optional))
        provided-namespace))
(defun provided-namespace (named-schema)
  "Returns the provided-namespace."
  (when (namespace-provided-p named-schema)
    (slot-value named-schema 'provided-namespace)))

(defgeneric namespace (named-schema)
  (:method ((instance named-schema))
    "Returns (values deduced provided provided-p)."
    (let ((deduced-namespace (deduced-namespace instance))
          (provided-namespace (provided-namespace instance))
          (namespace-provided-p (namespace-provided-p instance)))
      (values deduced-namespace provided-namespace namespace-provided-p))))

;; initialization

(defmethod initialize-instance :around
    ((instance named-schema) &rest initargs &key (aliases nil aliasesp))
  (let ((aliases (parse-aliases aliases aliasesp)))
    (setf (getf initargs :aliases) aliases))
  (apply #'call-next-method instance initargs))

(declaim
 (ftype (function (symbol list) (values t &optional)) %parse-name))
(defun %parse-name (name initargs)
  (let* ((initargs (cddr (member :name initargs)))
         (other-name (getf initargs :name name)))
    (if (eq name other-name)
        (string name)
        other-name)))
(declaim
 (ftype (function (t list) (values t &optional)) parse-name))
(defun parse-name (name initargs)
  (if (symbolp name)
      (%parse-name name initargs)
      name))
(defmethod initialize-instance :after
    ((instance named-schema)
     &rest initargs
     &key
       (name (error "Must supply NAME"))
       enclosing-namespace)
  (let ((provided-namespace (provided-namespace instance)))
    (with-slots (provided-name deduced-name deduced-namespace fullname) instance
      (setf provided-name (parse-name name initargs)
            deduced-name (fullname->name provided-name)
            deduced-namespace (deduce-namespace
                               provided-name provided-namespace enclosing-namespace)
            fullname (deduce-fullname
                      provided-name provided-namespace enclosing-namespace)))))

(defmethod scalarize-initargs
    ((metaclass (eql 'named-schema)) (initargs list))
  (let ((aliases (getf initargs :aliases)))
    (if (remf initargs :aliases)
        (list* :aliases aliases (scalarize-initargs 'complex-schema initargs))
        (scalarize-initargs 'complex-schema initargs))))
