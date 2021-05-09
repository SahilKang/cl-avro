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
(defpackage #:cl-avro.schema.io.write
  (:use #:cl)
  (:import-from #:cl-avro.schema.io.write.canonicalize
                #:canonicalize
                #:*seen*)
  (:import-from #:cl-avro.schema.io.common
                #:downcase-symbol)
  (:import-from #:cl-avro.schema.io.st-json
                #:convert-to-st-json)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.logical
                #:logical-schema
                #:underlying

                #:decimal
                #:scale
                #:precision

                #:duration)
  (:import-from #:cl-avro.schema.complex
                #:schema
                #:named-schema
                #:name
                #:fullname
                #:namespace
                #:aliases

                #:fixed
                #:size

                #:schemas

                #:items

                #:enum
                #:symbols
                #:default

                #:record
                #:fields

                #:field
                #:order)
  (:shadowing-import-from #:cl-avro.schema.complex
                          #:union
                          #:array
                          #:map
                          #:values
                          #:type)
  (:export #:schema->json))
(in-package #:cl-avro.schema.io.write)

(declaim
 (ftype (function (schema stream &optional boolean) (cl:values &optional))
        schema->json))
(defun schema->json (schema stream &optional canonical-form)
  "Write json representation of avro SCHEMA into STREAM.

If CANONICAL-FORM is true, then the Canonical Form is written."
  (let* ((*seen* (make-hash-table :test #'eq))
         (schema (if canonical-form
                     (canonicalize schema)
                     schema))
         (st-json:*output-literal-unicode*
           (or canonical-form
               st-json:*output-literal-unicode*)))
    (st-json:write-json (to-jso schema) stream))
  (cl:values))

(defmethod st-json:write-json-element
    ((vector simple-vector) (stream stream))
  (if (zerop (length vector))
      (write-string "[]" stream)
      (loop
        initially
           (write-char #\[ stream)
           (st-json:write-json-element (elt vector 0) stream)

        for i from 1 below (length vector)
        for elt = (elt vector i)
        do
           (write-char #\, stream)
           (st-json:write-json-element elt stream)

        finally
           (write-char #\] stream))))

(defgeneric to-jso (schema))

(macrolet
    ((defprimitives ()
       (flet ((make-defmethod (schema-name-pair)
                (destructuring-bind (schema . name)
                    schema-name-pair
                  `(defmethod to-jso ((schema (eql ',schema)))
                     (declare (ignore schema))
                     ,name))))
         `(progn
            ,@(mapcar #'make-defmethod +primitive->name+)))))
  (defprimitives))

(defmethod to-jso :before ((schema named-schema))
  (setf (gethash schema *seen*) t))

(defmethod to-jso :around ((schema named-schema))
  (if (gethash schema *seen*)
      (fullname schema) ;; prevents infinite recursion
      (let ((initargs (call-next-method))
            (aliases (aliases schema)))
        (when aliases
          (push aliases initargs)
          (push "aliases" initargs))
        (multiple-value-bind (_ namespace namespacep) (namespace schema)
          (declare (ignore _))
          (when namespacep
            (push namespace initargs)
            (push "namespace" initargs)))
        (push (downcase-symbol (class-name (class-of schema))) initargs)
        (push "type" initargs)
        (push (nth-value 1 (name schema)) initargs)
        (push "name" initargs)
        (apply #'st-json:jso initargs))))

(defmethod to-jso ((schema fixed))
  (list "size" (size schema)))

(defmethod to-jso ((schema union))
  (cl:map 'simple-vector #'to-jso (schemas schema)))

(defmethod to-jso ((schema array))
  (st-json:jso
   "type" "array"
   "items" (to-jso (items schema))))

(defmethod to-jso ((schema map))
  (st-json:jso
   "type" "map"
   "values" (to-jso (values schema))))

(defmethod to-jso ((schema enum))
  (let ((initargs (list "symbols" (symbols schema)))
        (documentation (documentation schema t))
        (default (default schema)))
    (when documentation
      (push documentation initargs)
      (push "doc" initargs))
    (when default
      (push default initargs)
      (push "default" initargs))
    initargs))

(defmethod to-jso ((schema record))
  (let ((initargs (list
                   "fields" (cl:map 'simple-vector #'to-jso (fields schema))))
        (documentation (documentation schema t)))
    (when documentation
      (push documentation initargs)
      (push "doc" initargs))
    initargs))

(defmethod to-jso ((field field))
  (let ((initargs (list
                   "name" (name field)
                   "type" (to-jso (type field))))
        (aliases (aliases field))
        (documentation (documentation field t)))
    (when aliases
      (push aliases initargs)
      (push "aliases" initargs))
    (when documentation
      (push documentation initargs)
      (push "doc" initargs))
    (multiple-value-bind (order orderp) (order field)
      (when orderp
        (push (downcase-symbol order) initargs)
        (push "order" initargs)))
    (multiple-value-bind (default defaultp) (default field)
      (when defaultp
        (push (convert-to-st-json default) initargs)
        (push "default" initargs)))
    (apply #'st-json:jso initargs)))

(defmethod to-jso ((schema logical-schema))
  (list
   "type" (to-jso (underlying schema))
   "logicalType" (downcase-symbol
                  (class-name
                   (if (typep schema '(or decimal duration))
                       (class-of schema)
                       schema)))))

(defmethod to-jso :around ((schema logical-schema))
  (let ((initargs (call-next-method)))
    (apply #'st-json:jso initargs)))

(defmethod to-jso ((schema decimal))
  (let ((initargs (call-next-method)))
    (multiple-value-bind (scale scalep) (scale schema)
      (when scalep
        (push scale initargs)
        (push "scale" initargs)))
    (push (precision schema) initargs)
    (push "precision" initargs)))
