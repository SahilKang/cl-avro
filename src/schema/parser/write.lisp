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
                        with package = (find-package 'cl-avro)
                        for symbol being the external-symbols of package
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

(defmethod %write-schema ((schema fixed-schema))
  (let ((hash-table (make-hash-table :test #'equal)))
    (setf (gethash "type" hash-table) "fixed"
          (gethash "name" hash-table) (name schema)
          (gethash "size" hash-table) (size schema))
    (when (namespace schema)
      (setf (gethash "namespace" hash-table) (namespace schema)))
    (when (aliases schema)
      (setf (gethash "aliases" hash-table) (coerce (aliases schema) 'list)))
    (st-json:write-json-to-string hash-table)))

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
  (let ((hash-table (make-hash-table :test #'equal)))
    (setf (gethash "type" hash-table) "enum"
          (gethash "name" hash-table) (name schema)
          (gethash "symbols" hash-table) (coerce (symbols schema) 'list))
    (when (default schema)
      (setf (gethash "default" hash-table) (default schema)))
    (when (namespace schema)
      (setf (gethash "namespace" hash-table) (namespace schema)))
    (when (aliases schema)
      (setf (gethash "aliases" hash-table) (coerce (aliases schema) 'list)))
    (when (doc schema)
      (setf (gethash "doc" hash-table) (doc schema)))
    (st-json:write-json-to-string hash-table)))

(defmethod %write-schema ((schema record-schema))
  (let ((hash-table (make-hash-table :test #'equal)))
    (setf (gethash "type" hash-table) "record"
          (gethash "name" hash-table) (name schema)
          (gethash "fields" hash-table) (map 'list
                                             (lambda (field)
                                               (st-json:read-json
                                                (%write-schema field)))
                                             (field-schemas schema)))
    (when (namespace schema)
      (setf (gethash "namespace" hash-table) (namespace schema)))
    (when (aliases schema)
      (setf (gethash "aliases" hash-table) (coerce (aliases schema) 'list)))
    (when (doc schema)
      (setf (gethash "doc" hash-table) (doc schema)))
    (st-json:write-json-to-string hash-table)))

(defmethod %write-schema ((schema field-schema))
  (let ((hash-table (make-hash-table :test #'equal)))
    (setf (gethash "name" hash-table) (name schema)
          (gethash "type" hash-table) (st-json:read-json
                                       (%write-schema (field-type schema))))
    (when (aliases schema)
      (setf (gethash "aliases" hash-table) (coerce (aliases schema) 'list)))
    (when (doc schema)
      (setf (gethash "doc" hash-table) (doc schema)))
    (unless (string= (order schema) "ascending")
      (setf (gethash "order" hash-table) (order schema)))
    (when (default schema)
      (setf (gethash "default" hash-table) (default schema)))
    (st-json:write-json-to-string hash-table)))
