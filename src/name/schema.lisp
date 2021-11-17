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
(defpackage #:cl-avro.internal.name.schema
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:mop #:cl-avro.internal.mop)
   (#:type #:cl-avro.internal.name.type)
   (#:deduce #:cl-avro.internal.name.deduce)
   (#:class #:cl-avro.internal.name.class))
  (:import-from #:cl-avro.internal.defprimitive
                #:*primitives*)
  (:import-from #:cl-avro.internal.type
                #:ufixnum)
  (:export #:named-schema
           #:valid-name
           #:valid-fullname
           #:array<alias>
           #:array<alias>?))
(in-package #:cl-avro.internal.name.schema)

;;; types

(macrolet
    ((make-or-form (name start)
       (declare (symbol name start))
       (labels
           ((to-name (schema)
              (string-downcase (string schema)))
            (to-string= (schema)
              `(string= ,name ,(to-name schema) :start1 ,start)))
         `(or ,@(mapcar #'to-string= *primitives*)))))
  (declaim
   (ftype (function (type:fullname ufixnum) (values boolean &optional))
          primitive-name-p))
  (defun primitive-name-p (name start)
    (make-or-form name start)))

(declaim (ftype (function (t) (values boolean &optional)) not-primitive-name-p))
(defun not-primitive-name-p (name)
  (not
   (when (simple-string-p name)
     (let* ((last-dot-position (position #\. name :test #'char= :from-end t))
            (start (if last-dot-position (1+ last-dot-position) 0)))
       (primitive-name-p name start)))))

(deftype valid-name ()
  "A NAME which does not name a primitive schema."
  '(and type:name (satisfies not-primitive-name-p)))

(deftype valid-fullname ()
  "A FULLNAME which does not name a primitive schema in any namespace."
  '(and type:fullname (satisfies not-primitive-name-p)))

;;; named-schema

(deftype array<alias> ()
  '(simple-array valid-fullname (*)))

(deftype array<alias>? ()
  '(or null array<alias>))

(defclass named-schema (class:named-class api:complex-schema)
  ((class:provided-name
    :type valid-fullname)
   (class:deduced-name
    :type valid-name)
   (class:fullname
    :type valid-fullname)
   (aliases
    :reader api:aliases
    :type array<alias>?
    :documentation "A vector of aliases if provided, otherwise nil."))
  (:metaclass mop:scalar-class)
  (:scalars :enclosing-namespace)
  (:documentation
   "Base class for named schema metaclasses."))

(defmethod closer-mop:validate-superclass
    ((class named-schema) (superclass class:named-class))
  t)

(defmethod closer-mop:validate-superclass
    ((class named-schema) (superclass class:named-class))
  t)

(declaim
 (ftype (function (sequence) (values list &optional)) internal:duplicates))
(defun internal:duplicates (sequence)
  "Return duplicate strings found in SEQUENCE."
  (let ((string->count (make-hash-table :test #'equal))
        duplicates)
    (flet ((process (string)
             (let ((count (incf (gethash string string->count 0))))
               (when (= count 2)
                 (push string duplicates)))))
      (map nil #'process sequence))
    duplicates))

(declaim (ftype (function (t) (values valid-fullname &optional)) parse-alias))
(defun parse-alias (alias)
  (check-type alias valid-fullname)
  alias)

(declaim
 (ftype (function (sequence boolean) (values array<alias>? &optional))
        parse-aliases))
(defun parse-aliases (aliases aliasesp)
  (when aliasesp
    (assert (null (internal:duplicates aliases)) (aliases))
    (map 'array<alias> #'parse-alias aliases)))

(declaim
 (ftype (function (named-schema type:namespace)
                  (values type:namespace &optional))
        re-deduce-namespace))
(defun re-deduce-namespace (schema enclosing-namespace)
  (let ((provided-name (class:provided-name schema))
        (provided-namespace (class:provided-namespace schema)))
    (deduce:deduce-namespace
     provided-name provided-namespace enclosing-namespace)))

(declaim
 (ftype (function (named-schema type:namespace)
                  (values valid-fullname &optional))
        re-deduce-fullname))
(defun re-deduce-fullname (schema enclosing-namespace)
  (let ((provided-name (class:provided-name schema))
        (provided-namespace (class:provided-namespace schema)))
    (deduce:deduce-fullname
     provided-name provided-namespace enclosing-namespace)))

(mop:definit ((instance named-schema) :after
              &key (aliases nil aliasesp) enclosing-namespace)
  (with-slots
        (class:deduced-namespace
         class:fullname
         (aliases-slot aliases))
      instance
    (setf aliases-slot (parse-aliases aliases aliasesp)
          class:deduced-namespace (re-deduce-namespace
                                   instance enclosing-namespace)
          class:fullname (re-deduce-fullname instance enclosing-namespace))))

;;; jso

(defmethod internal:read-jso
    ((jso string) fullname->schema enclosing-namespace)
  (let ((fullname (deduce:deduce-fullname jso nil enclosing-namespace)))
    (or (nth-value 0 (gethash fullname fullname->schema))
        (error "Unknown schema with name: ~S" jso))))

(defmethod internal:write-jso :before
    ((schema named-schema) seen canonical-form-p)
  (setf (gethash schema seen) t))

(defmethod internal:write-jso :around
    ((schema named-schema) seen canonical-form-p)
  (let ((name (if canonical-form-p
                  (api:fullname schema)
                  (nth-value 1 (api:name schema)))))
    (if (gethash schema seen)
        name
        (let ((initargs (list* "name" name
                               "type" (internal:downcase-symbol
                                       (class-name (class-of schema)))
                               (call-next-method)))
              (aliases (api:aliases schema)))
          (unless canonical-form-p
            (when aliases
              (push aliases initargs)
              (push "aliases" initargs))
            (multiple-value-bind (deduced namespace namespacep)
                (api:namespace schema)
              (declare (ignore deduced))
              (when namespacep
                (push namespace initargs)
                (push "namespace" initargs))))
          (apply #'st-json:jso initargs)))))
