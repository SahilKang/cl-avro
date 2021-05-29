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
  (:local-nicknames
   (#:named #:cl-avro.schema.complex.named.class))
  (:import-from #:cl-avro.schema.complex.common
                #:assert-distinct
                #:define-initializers)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema)
  (:import-from #:cl-avro.schema.complex.named.type
                #:name
                #:namespace
                #:fullname
                #:deduce-namespace
                #:deduce-fullname)
  (:import-from #:cl-avro.schema.complex.scalarize
                #:scalarize-class)
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

(defclass named-schema (named:named-class complex-schema)
  ((named:provided-name
    :type valid-fullname)
   (named:deduced-name
    :type valid-name)
   (named:fullname
    :type valid-fullname)
   (aliases
    :reader aliases
    :type (or null (simple-array valid-fullname (*)))
    :documentation "A vector of aliases if provided, otherwise nil."))
  (:metaclass scalarize-class)
  (:scalarize :enclosing-namespace)
  (:documentation
   "Base class for avro named schemas."))

(defmethod closer-mop:validate-superclass
    ((class named-schema) (superclass complex-schema))
  t)

(defmethod closer-mop:validate-superclass
    ((class named-schema) (superclass named:named-class))
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

;; initialization

(declaim
 (ftype (function (named-schema namespace) (values namespace &optional))
        re-deduce-namespace))
(defun re-deduce-namespace (schema enclosing-namespace)
  (let ((provided-name (named:provided-name schema))
        (provided-namespace (named:provided-namespace schema)))
    (deduce-namespace provided-name provided-namespace enclosing-namespace)))

(declaim
 (ftype (function (named-schema namespace) (values valid-fullname &optional))
        re-deduce-fullname))
(defun re-deduce-fullname (schema enclosing-namespace)
  (let ((provided-name (named:provided-name schema))
        (provided-namespace (named:provided-namespace schema)))
    (deduce-fullname provided-name provided-namespace enclosing-namespace)))

(define-initializers named-schema :after
    (&key (aliases nil aliasesp) enclosing-namespace)
  (with-slots
        (named:deduced-namespace
         named:fullname
         (aliases-slot aliases))
      instance
    (setf aliases-slot (parse-aliases aliases aliasesp)
          named:deduced-namespace (re-deduce-namespace
                                   instance enclosing-namespace)
          named:fullname (re-deduce-fullname instance enclosing-namespace))))
