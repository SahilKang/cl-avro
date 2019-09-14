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

;;; good old-fashioned, gmo-free, grass-fed recursive-descent parser
;;; handcrafted with love in San Francisco

(defun json->schema (json)
  "Return an avro schema according to JSON. JSON is a string or stream."
  (let ((*fullname->schema* (make-schema-hash-table)))
    (declare (special *fullname->schema*))
    (parse-schema (st-json:read-json json))))

(defun schema->json (schema)
  "Return the json string representation of avro SCHEMA."
  (let ((*schema->name* (make-hash-table :test #'eq)))
    (declare (special *schema->name*))
    (%write-schema schema)))


;;; read into schema object:

(defparameter *current-namespace* nil)

(defun find-schema (name)
  (loop
     with schema-name = (string-upcase (concatenate 'string name "-schema"))

     for s being the external-symbols of (find-package "CL-AVRO")
     when (string= schema-name (symbol-name s))
     return s))

(defun make-schema-hash-table ()
  "Return a hash-table mapping avro primitive type names to schemas.

This hash-table is meant to be added to during the course of schema
parsing."
  (loop
     with fullname->schema = (make-hash-table :test #'equal)

     for name in '("null" "boolean" "int" "long" "float" "double" "bytes" "string")
     for schema = (find-schema name)

     if (null schema)
     do (error "~&Could not find schema for name: ~A" name)
     else do (setf (gethash name fullname->schema) schema)

     finally (return fullname->schema)))

(defun parse-schema (json)
  (etypecase json
    (list (parse-union json))
    (string (parse-string json))
    (st-json:jso (parse-jso json))))

(defun parse-string (fullname)
  (declare (special *fullname->schema*))
  (multiple-value-bind (schema schemap) (gethash fullname *fullname->schema*)
    (unless schemap
      (error "~&Unknown schema with fullname: ~A" fullname))
    schema))

(defun parse-union (union-list)
  (loop
     with seen-array-p = nil
     with seen-map-p = nil

     for elem in union-list

     if (typep elem 'list)
     do (error "~&Union ~A contains an immediate union: ~A" union-list elem)

     else if (typep elem 'string)
     collect (parse-string elem) into schemas

     else if (typep elem 'st-json:jso)
     collect (let ((type (st-json:getjso "type" elem)))
               (cond
                 ((equal type "array")
                  (if seen-array-p
                      (error "~&Union ~A has more than one array" union-list)
                      (setf seen-array-p t)))
                 ((equal type "map")
                  (if seen-map-p
                      (error "~&Union ~A has more than one map" union-list)
                      (setf seen-map-p t))))
               (parse-jso elem)) into schemas

     finally (return (make-instance 'union-schema
                                    :schemas (coerce schemas 'vector)))))

(defun parse-jso (jso)
  (declare (special *fullname->schema*))
  (let ((type (st-json:getjso "type" jso)))
    (etypecase type
      (list (parse-union type))  ; TODO does this occur/is it allowed?
      (st-json:jso (parse-jso type)) ; TODO does this recurse properly?
      (string
       (cond
         ((nth-value 1 (gethash type *fullname->schema*))
          (parse-string type))
         ((string= type "record") (parse-record jso))
         ((string= type "enum") (parse-enum jso))
         ((string= type "array") (parse-array jso))
         ((string= type "map") (parse-map jso))
         ((string= type "fixed") (parse-fixed jso))
         (t (error "~&Unknown type: ~A" type)))))))

(defun figure-out-fullname (name namespace)
  (cond
    ((position #\. name) name)
    ((not (zerop (length namespace)))
     (concatenate 'string namespace "." name))
    ((not (zerop (length *current-namespace*)))
     (concatenate 'string *current-namespace* "." name))
    (t name)))

;; some schema objects have name but not namespace
;; this prevents a NO-APPLICABLE-METHOD-ERROR
(defmethod namespace (schema)
  nil)

(defun register-named-schema (schema)
  (declare (special *fullname->schema*))
  (let ((fullname (figure-out-fullname (name schema) (namespace schema))))
    (when (nth-value 1 (gethash fullname *fullname->schema*))
      (error "~&Name is already taken: ~A" fullname))
    (setf (gethash fullname *fullname->schema*) schema)))

(defmacro with-fields (fields jso &body body)
  "The symbols in FIELDS are downcased before the st-json:getjso call."
  (let ((j (gensym)))
    `(let* ((,j ,jso)
            ,@(mapcar
               (lambda (binding)
                 (list binding
                       `(st-json:getjso ,(string-downcase (string binding)) ,j)))
               fields))
       ,@body)))

;; TODO handle shit data

(defun parse-enum (jso)
  (with-fields (name namespace aliases doc symbols default) jso
    (register-named-schema
     (make-instance 'enum-schema
                    :name name
                    :namespace namespace
                    :aliases aliases
                    :doc doc
                    :symbols symbols
                    :default default))))

(defun parse-fixed (jso)
  (with-fields (name namespace aliases size) jso
    (register-named-schema
     (make-instance 'fixed-schema
                    :name name
                    :namespace namespace
                    :aliases aliases
                    :size size))))

(defun parse-array (jso)
  (with-fields (items) jso
    (make-instance 'array-schema
                   :item-schema (parse-schema items))))

(defun parse-map (jso)
  (with-fields (values) jso
    (make-instance 'map-schema
                   :value-schema (parse-schema values))))

(defun figure-out-namespace (name &optional namespace)
  (let ((pos (position #\. name :from-end t)))
    (cond
      (pos
       (subseq name (1+ pos)))
      ((not (zerop (length namespace)))
       namespace)
      (t
       *current-namespace*))))

(defun parse-record (jso)
  (with-fields (name namespace doc aliases fields) jso
    ;; record schemas can be recursive (fields can refer to the record
    ;; type), so let's register the record schema and then mutate the
    ;; fields vector
    (let* ((field-schemas (make-array (length fields)
                                      :element-type 'avro-schema
                                      :initial-element 'null-schema
                                      :fill-pointer 0))
           (record (make-instance 'record-schema
                                  :name name
                                  :namespace namespace
                                  :doc doc
                                  :aliases (coerce aliases 'vector)
                                  :field-schemas field-schemas)))
      (let ((*current-namespace* (figure-out-namespace name namespace)))
        (register-named-schema record)
        (loop
           for field-jso in fields
           for schema = (parse-field field-jso)
           do (vector-push schema field-schemas))
        record))))

(defun parse-field (jso)
  (with-fields (name doc type order aliases default) jso
    (let ((*current-namespace* (figure-out-namespace name)))
      (register-named-schema
       (make-instance 'field-schema
                      :name name
                      :doc doc
                      :field-type (parse-schema type)
                      :order (or order "ascending")
                      :aliases aliases
                      :default default)))))


;;; write schema object to json string:

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
