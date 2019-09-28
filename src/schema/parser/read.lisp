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


(defparameter *current-namespace* "")

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
      (list (parse-union type))
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
  (values "" nil))

(defun register-named-schema (schema)
  (declare (special *fullname->schema*))
  (let ((fullname (figure-out-fullname (name schema) (namespace schema))))
    (when (nth-value 1 (gethash fullname *fullname->schema*))
      (error "~&Name is already taken: ~A" fullname))
    (setf (gethash fullname *fullname->schema*) schema)))

(defmacro +p (symbol)
  "Returns SYMBOL with a 'p' appended to it."
  `(read-from-string (format nil "~Sp" ,symbol)))

(defmacro with-fields ((&rest fields) jso &body body)
  "Binds FIELDS as well as its elements suffixed with 'p'."
  (flet ((->string (symbol)
           (string-downcase (string symbol))))
    (let* ((j (gensym))
           (fieldsp (mapcar (lambda (field) (+p field)) fields))
           (mvb-forms (mapcar
                       (lambda (field fieldp)
                         (let ((value (gensym))
                               (valuep (gensym)))
                           `(multiple-value-bind (,value ,valuep)
                                (st-json:getjso ,(->string field) ,j)
                              (setf ,field ,value
                                    ,fieldp ,valuep))))
                       fields
                       fieldsp)))
      `(let ((,j ,jso)
             ,@fields
             ,@fieldsp)
         ,@mvb-forms
         ,@body))))

(defmacro make-kwargs ((&rest always-included) &rest included-only-when-non-nil)
  "Expects p-suffixed symbols of INCLUDED-ONLY-WHEN-NON-NIL to also exist."
  (flet ((->keyword (symbol)
           (intern (symbol-name symbol) 'keyword)))
    (let* ((args (gensym))
           (initial-args (loop
                            for symbol in always-included
                            collect (->keyword symbol)
                            collect symbol))
           (when-forms (mapcar
                        (lambda (symbol)
                          `(when ,(+p symbol)
                             (push ,symbol ,args)
                             (push ,(->keyword symbol) ,args)))
                        included-only-when-non-nil)))
      `(let ((,args (list ,@initial-args)))
         ,@when-forms
         ,args))))

(defun parse-enum (jso)
  (with-fields (name namespace aliases doc symbols default) jso
    (register-named-schema
     (apply #'make-instance
            'enum-schema
            (make-kwargs (name symbols default) namespace aliases doc)))))

(defun parse-fixed (jso)
  (with-fields (name namespace aliases size) jso
    (register-named-schema
     (apply #'make-instance
            'fixed-schema
            (make-kwargs (name size) namespace aliases)))))

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
           (record (apply
                    #'make-instance
                    'record-schema
                    (make-kwargs (name field-schemas) namespace doc aliases))))
      (let ((*current-namespace* (figure-out-namespace name namespace)))
        (register-named-schema record)
        (loop
           for field-jso in fields
           for schema = (parse-field field-jso)
           do (vector-push schema field-schemas))
        record))))

(defun parse-field (jso)
  (with-fields (name doc type order aliases default) jso
    (let* ((*current-namespace* (figure-out-namespace name))
           (schema (parse-schema type))
           (args (list :name name
                       :field-type schema)))
      (when order
        (push order args)
        (push :order args))
      (when aliases
        (push aliases args)
        (push :aliases args))
      (when doc
        (push doc args)
        (push :doc args))
      (when default
        (push (parse-default schema default) args)
        (push :default args))
      (register-named-schema
       (apply #'make-instance 'field-schema args)))))

(defun parse-default (schema default)
  (etypecase schema

    (symbol
     (parse-primitive-default schema default))

    (enum-schema
     (if (validp schema default)
         default
         (error "~&Bad default value ~S for ~S" default (type-of schema))))

    (array-schema
     (let ((default (mapcar (lambda (x)
                              (parse-default (item-schema schema) x))
                            default)))
       (if (validp schema default)
           default
           (error "~&Bad default value ~S for ~S" default (type-of schema)))))

    (map-schema
     (let ((default (make-hash-table :test #'equal)))
       (st-json:mapjso (lambda (k v)
                         (setf (gethash k default)
                               (parse-default (value-schema schema) v)))
                       default)
       (if (validp schema default)
           default
           (error "~&Bad default value ~S for ~S" default (type-of schema)))))

    (fixed-schema
     (check-type default string)
     (babel:string-to-octets default :encoding :latin-1))

    (union-schema
     (let ((default (parse-default (elt (schemas schema) 0) default)))
       (if (validp (elt (schemas schema) 0) default)
           default
           (error "~&Bad default value ~S for ~S" default (type-of schema)))))

    (record-schema
     (check-type default st-json:jso)
     (let ((hash-table (make-hash-table :test #'equal))
           (fields (field-schemas schema)))
       (st-json:mapjso (lambda (k v) (setf (gethash k hash-table) v)) default)
       (unless (= (length fields) (hash-table-count hash-table))
         (error "~&Bad default value ~S for ~S" default (type-of schema)))
       (loop
          with vector = (make-array (length fields) :fill-pointer 0)

          for field across fields

          for type = (field-type field)
          for name = (name field)
          for (value valuep) = (multiple-value-list (gethash name hash-table))

          if (not valuep)
          do (error "~&Field ~S does not exist in default value" name)
          else do (vector-push (parse-default type value) vector)

          finally (return vector))))))

(defun parse-primitive-default (schema default)
  (ecase schema

    (null-schema
     (if (eq default :null)
         nil
         (error "~&Bad default value ~S for ~S" default schema)))

    (boolean-schema
     (st-json:from-json-bool default))

    ((int-schema long-schema string-schema)
     (if (validp schema default)
         default
         (error "~&Bad default value ~S for ~S" default schema)))

    (float-schema
     (coerce default 'single-float))

    (double-schema
     (coerce default 'double-float))

    (bytes-schema
     (check-type default string)
     (babel:string-to-octets default :encoding :latin-1))))
