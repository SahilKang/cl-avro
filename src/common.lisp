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

(in-package #:cl-avro)

(defun deduce-fullname (name namespace enclosing-namespace)
  (cond
    ((position #\. name)
     name)
    ((not (zerop (length namespace)))
     (concatenate 'string namespace "." name))
    ((not (zerop (length enclosing-namespace)))
     (concatenate 'string enclosing-namespace "." name))
    (t name)))

(defun deduce-namespace (name namespace enclosing-namespace)
  (let ((pos (position #\. name :from-end t)))
    (cond
      (pos
       (subseq name 0 pos))
      ((not (zerop (length namespace)))
       namespace)
      (t
       enclosing-namespace))))

(macrolet
    ((assign-primitive-schemas-list (param-name)
       (let* ((names '("NULL-SCHEMA"
                       "BOOLEAN-SCHEMA"
                       "INT-SCHEMA"
                       "LONG-SCHEMA"
                       "FLOAT-SCHEMA"
                       "DOUBLE-SCHEMA"
                       "BYTES-SCHEMA"
                       "STRING-SCHEMA"))
              (schemas (mapcar
                        (lambda (name)
                          (multiple-value-bind (symbol status)
                              (find-symbol name)
                            (unless (eq status :external)
                              (error "~&~S is ~S, not :EXTERNAL" name status))
                            symbol))
                        names)))
         `(defparameter ,param-name ',schemas))))
  (assign-primitive-schemas-list +primitive-schemas+))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun downcase-symbol (symbol)
    (declare (symbol symbol))
    "Return downcased string from SYMBOL."
    (string-downcase (symbol-name symbol))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun schema->name (schema)
    (declare (symbol schema))
    "Return the name of SCHEMA."
    (let* ((string (downcase-symbol schema))
           (hyphen-pos (position #\- string :test #'char= :from-end t)))
      (unless (uiop:string-suffix-p string "-schema")
        (error "~&~S does not end in '-schema'" schema))
      (subseq string 0 hyphen-pos))))

(defmacro defmethods-for-primitives
    (method-name schema-name (&rest lambda-list) &body body)
  "Expand to defmethod forms for each avro primitive schema.

For each avro primitive schema, a defmethod form will be generated accordingly:

  * METHOD-NAME defines the method name
  * LAMBDA-LIST's symbols will each be eql specialized to the primitive schema
  * BODY indicates the method body.

For each body, if SCHEMA-NAME is non-nil, then it is bound to the
schema->name of the primitive schema."
  (let ((defmethods (mapcar
                     (lambda (schema)
                       (let ((specialized-args (mapcar
                                                (lambda (arg)
                                                  `(,arg (eql ',schema)))
                                                lambda-list))
                             (body
                              (if schema-name
                                  `((let ((,schema-name ,(schema->name schema)))
                                      ,@body))
                                  body)))
                         `(defmethod ,method-name ,specialized-args
                            ,@body)))
                     +primitive-schemas+)))
    `(progn
       ,@defmethods)))
