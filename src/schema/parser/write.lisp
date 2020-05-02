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

(defun schema->json (schema &optional canonical-form)
  "Return the json string representation of avro SCHEMA.

If CANONICAL-FORM is true, then return the Canonical Form as defined
in the avro spec."
  (let ((*schema->name* (make-hash-table :test #'eq))
        (st-json:*output-literal-unicode*
         (or canonical-form
             st-json:*output-literal-unicode*)))
    (declare (special *schema->name*))
    (%write-schema (if canonical-form
                       (canonicalize schema)
                       schema))))


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
        (st-json:write-json-to-string
         (call-next-method)))))

;; specialize %write-schema methods for primitive avro types:
(defmethods-for-primitives %write-schema schema-name (schema)
  schema-name)

(defmacro append-optionals (schema args &rest symbols)
  "Append optional fields to ARGS when they're non-nil and return ARGS."
  (let ((mvb-forms (mapcar
                    (lambda (symbol)
                      (let ((value (gensym))
                            (valuep (gensym))
                            (field (downcase-symbol symbol)))
                        `(multiple-value-bind (,value ,valuep)
                             (,symbol ,schema)
                           (when ,valuep
                             (setf ,args
                                   (nconc ,args (list ,field ,value)))))))
                    symbols)))
    `(progn
       ,@mvb-forms
       ,args)))

(defmacro make-fields (schema (&rest required-fields) &rest optional-fields)
  (let ((fields (gensym))
        (initial (apply #'nconc
                        (mapcar
                         (lambda (s)
                           (declare ((or string symbol) s))
                           (if (stringp s)
                               (list "type" s)
                               (list (downcase-symbol s) (list s schema))))
                         required-fields))))
    `(let ((,fields (list ,@initial)))
       (append-optionals ,schema ,fields ,@optional-fields))))

(defmethod st-json:write-json-element ((element vector) stream)
  (st-json:write-json-element (coerce element 'list) stream))

(defmethod %write-schema ((schema fixed-schema))
  (apply #'st-json:jso
         (make-fields schema (name "fixed" size) namespace aliases)))

(defmethod %write-schema ((schema union-schema))
  (map 'list
       (lambda (schema)
         (st-json:read-json (%write-schema schema)))
       (schemas schema)))

(defmethod %write-schema ((schema array-schema))
  (st-json:jso
   "type" "array"
   "items" (st-json:read-json
            (%write-schema (item-schema schema)))))

(defmethod %write-schema ((schema map-schema))
  (st-json:jso
   "type" "map"
   "values" (st-json:read-json
             (%write-schema (value-schema schema)))))

(defmethod %write-schema ((schema enum-schema))
  (apply
   #'st-json:jso
   (make-fields schema (name "enum" symbols) default namespace aliases doc)))

(defmethod %write-schema ((schema record-schema))
  (let ((args (make-fields schema (name "record") namespace aliases doc))
        (fields (map 'list
                     (lambda (field)
                       (st-json:read-json
                        (%write-schema field)))
                     (field-schemas schema))))
    ;; field order only matters when we're writing to canonical form
    ;; and since CANONICALIZE sets the optional namespace, aliases,
    ;; and doc fields to nil in these cases, we can append fields to
    ;; args safely
    (apply #'st-json:jso (nconc args (list "fields" fields)))))

(defmethod %write-schema ((schema field-schema))
  (let ((args (list "name" (name schema)
                    "type" (st-json:read-json
                            (%write-schema (field-type schema))))))
    (append-optionals schema args aliases doc order)
    (multiple-value-bind (default defaultp)
        (default schema)
      (when defaultp
        (let ((default (write-default (field-type schema) default)))
          (setf args (nconc args (list "default" default))))))
    (apply #'st-json:jso args)))

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

(defmethod %write-schema ((schema decimal-schema))
  (st-json:jso
   "type" (st-json:read-json
           (%write-schema (underlying-schema schema)))
   "logicalType" "decimal"
   "precision" (precision schema)
   "scale" (scale schema)))

(defmethod %write-schema ((schema (eql 'uuid-schema)))
  (st-json:jso
   "type" "string"
   "logicalType" "uuid"))

(defmethod %write-schema ((schema (eql 'date-schema)))
  (st-json:jso
   "type" "int"
   "logicalType" "date"))

(defmethod %write-schema ((schema (eql 'time-millis-schema)))
  (st-json:jso
   "type" "int"
   "logicalType" "time-millis"))

(defmethod %write-schema ((schema (eql 'time-micros-schema)))
  (st-json:jso
   "type" "long"
   "logicalType" "time-micros"))

(defmethod %write-schema ((schema (eql 'timestamp-millis-schema)))
  (st-json:jso
   "type" "long"
   "logicalType" "timestamp-millis"))

(defmethod %write-schema ((schema (eql 'timestamp-micros-schema)))
  (st-json:jso
   "type" "long"
   "logicalType" "timestamp-micros"))

(defmethod %write-schema ((schema duration-schema))
  (st-json:jso
   "type" (st-json:read-json
           (%write-schema (underlying-schema schema)))
   "logicalType" "duration"))
