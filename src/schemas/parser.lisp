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

(defun read-schema (json)
  "Return an avro schema according to JSON. JSON is a string or stream."
  (parse-schema (st-json:read-json json)))


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

(defun parse-schema (json &optional fullname->schema)
  (let ((*fullname->schema* (or fullname->schema (make-schema-hash-table))))
    (declare (special *fullname->schema*))
    (etypecase json
      (list (parse-union json))
      (string (parse-string json))
      (st-json:jso (parse-jso json)))))

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
  (declare (special *fullname->schema*))
  (with-fields (items) jso
    (make-instance 'array-schema
                   :item-schema (parse-schema items *fullname->schema*))))

(defun parse-map (jso)
  (declare (special *fullname->schema*))
  (with-fields (values) jso
    (make-instance 'map-schema
                   :value-schema (parse-schema values *fullname->schema*))))

(defun update-current-namespace (name &optional namespace)
  (let ((pos (position #\. name :from-end t)))
    (cond
      (pos
       (setf *current-namespace* (subseq name (1+ pos))))
      ((not (zerop (length namespace)))
       (setf *current-namespace* namespace)))))

(defun parse-record (jso)
  (with-fields (name namespace doc aliases fields) jso
    ;; record schemas can be recursive (fields can refer to the record
    ;; type), so let's register the record schema and then mutate the
    ;; fields vector
    (let* ((field-schemas (make-array (length fields)))
           (record (make-instance 'record-schema
                                  :name name
                                  :namespace namespace
                                  :doc doc
                                  :aliases aliases
                                  :field-schemas field-schemas)))
      (update-current-namespace name namespace)
      (register-named-schema record)
      (loop
         for field-jso in fields and i from 0
         for schema = (parse-field field-jso)
         do (setf (elt field-schemas i) schema))
      record)))

(defun parse-field (jso)
  (declare (special *fullname->schema*))
  (with-fields (name doc type order aliases default) jso
    (update-current-namespace name)
    (register-named-schema
     (make-instance 'field-schema
                    :name name
                    :doc doc
                    :field-type (parse-schema type *fullname->schema*)
                    :order order
                    :aliases aliases
                    :default default))))
