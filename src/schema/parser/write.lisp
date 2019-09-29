;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defun schema->json (schema)
  "Return the json string representation of avro SCHEMA."
  (let ((*schema->name* (make-hash-table :test #'eq)))
    (declare (special *schema->name*))
    (%write-schema schema)))


(defgeneric %write-schema (schema))

;; these :before and :around methods prevent infinite recursion when
;; writing a recursive schema

(defmethod %write-schema :before ((schema named-type))
  (declare (special *schema->name*))
  (setf (gethash schema *schema->name*) (name schema)))

(defmethod %write-schema :around (schema)
  (declare (special *schema->name*))
  (let ((name (gethash schema *schema->name*)))
    (if name
        (format nil "~S" name)
        (call-next-method))))

;; specialize %write-schema methods for primitive avro types:
(macrolet
    ((defmethods (&rest schema-strings)
       (let* ((->symbol (lambda (s)
                          (read-from-string (concatenate 'string s "-schema"))))
              (pairs (loop
                        for symbol being the external-symbols of 'cl-avro
                        for string = (find-if (lambda (s)
                                                (eq symbol (funcall ->symbol s)))
                                              schema-strings)
                        when string
                        collect (list string symbol))))
         (unless (= (length pairs) (length schema-strings))
           (error "~&Not all schemas were found"))
         `(progn
            ,@(loop
                 for (string symbol) in pairs
                 collect `(defmethod %write-schema ((schema (eql ',symbol)))
                            ,(format nil "~S" string)))))))
  (defmethods "null" "boolean" "int" "long" "float" "double" "bytes" "string"))

(defmacro make-ht (schema type-field (&rest always-set) &rest set-only-when-non-nil)
  (declare (string type-field))
  (flet ((->string (symbol)
           (string-downcase (string symbol))))
    (let* ((hash-table (gensym))
           (setf-pairs `((gethash "type" ,hash-table) ,type-field
                         ,@(loop
                              for symbol in always-set
                              collect `(gethash ,(->string symbol) ,hash-table)
                              collect (list symbol schema))))
           (mvb-forms (mapcar
                       (lambda (symbol)
                         (let ((value (gensym))
                               (valuep (gensym))
                               (field (->string symbol)))
                           `(multiple-value-bind (,value ,valuep)
                                (,symbol ,schema)
                              (when ,valuep
                                (setf (gethash ,field ,hash-table) ,value)))))
                       set-only-when-non-nil)))
      `(let ((,hash-table (make-hash-table :test #'equal)))
         (setf ,@setf-pairs)
         ,@mvb-forms
         ,hash-table))))

(defmethod st-json:write-json-element ((element vector) stream)
  (st-json:write-json-element (coerce element 'list) stream))

(defmethod %write-schema ((schema fixed-schema))
  (st-json:write-json-to-string
   (make-ht schema "fixed" (name size) namespace aliases)))

(defmethod %write-schema ((schema union-schema))
  (st-json:write-json-to-string
   (map 'list
        (lambda (schema)
          (st-json:read-json (%write-schema schema)))
        (schemas schema))))

(defmethod %write-schema ((schema array-schema))
  (st-json:write-json-to-string
   (st-json:jso
    "type" "array"
    "items" (st-json:read-json
             (%write-schema (item-schema schema))))))

(defmethod %write-schema ((schema map-schema))
  (st-json:write-json-to-string
   (st-json:jso
    "type" "map"
    "values" (st-json:read-json
              (%write-schema (value-schema schema))))))

(defmethod %write-schema ((schema enum-schema))
  (st-json:write-json-to-string
   (make-ht schema "enum" (name symbols) default namespace aliases doc)))

(defmethod %write-schema ((schema record-schema))
  (let ((hash-table (make-ht schema "record" (name) namespace aliases doc)))
    (setf (gethash "fields" hash-table)
          (map 'list
               (lambda (field)
                 (st-json:read-json
                  (%write-schema field)))
               (field-schemas schema)))
    (st-json:write-json-to-string hash-table)))

(defmethod %write-schema ((schema field-schema))
  (let ((hash-table (make-hash-table :test #'equal)))
    (setf (gethash "name" hash-table) (name schema)
          (gethash "type" hash-table) (st-json:read-json
                                       (%write-schema (field-type schema))))
    (multiple-value-bind (aliases aliasesp) (aliases schema)
      (when aliasesp
        (setf (gethash "aliases" hash-table) aliases)))
    (multiple-value-bind (doc docp) (doc schema)
      (when docp
        (setf (gethash "doc" hash-table) doc)))
    (multiple-value-bind (order orderp) (order schema)
      (when orderp
        (setf (gethash "order" hash-table) order)))
    (multiple-value-bind (default defaultp) (default schema)
      (when defaultp
        (setf (gethash "default" hash-table)
              (write-default (field-type schema) default))))
    (st-json:write-json-to-string hash-table)))

(defun write-default (schema default)
  (etypecase schema

    (symbol
     (get-primitive-default schema default))

    (enum-schema
     default)

    (array-schema
     (map 'list
          (lambda (x)
            (write-default (item-schema schema) x))
          default))

    (map-schema
     (let ((hash-table (make-hash-table :test #'equal)))
       (maphash (lambda (k v)
                  (setf (gethash k hash-table)
                        (write-default (value-schema schema) v)))
                default)
       hash-table))

    (fixed-schema
     (babel:octets-to-string default :encoding :latin-1))

    (union-schema
     (write-default (elt (schemas schema) 0) default))

    (record-schema
     (loop
        with hash-table = (make-hash-table :test #'equal)
        for field across (field-schemas schema)
        for value across default

        for type = (field-type field)
        for name = (name field)

        do (setf (gethash name hash-table) (write-default type value))

        finally (return hash-table)))))

(defun get-primitive-default (schema default)
  (ecase schema

    (null-schema
     :null)

    (boolean-schema
     (st-json:from-json-bool default))

    ((or int-schema long-schema string-schema float-schema double-schema)
     default)

    (bytes-schema
     (babel:octets-to-string default :encoding :latin-1))))
