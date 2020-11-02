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

(defun* schema->json (schema &optional canonical-form)
    ((avro-schema &optional boolean) (values simple-string &optional))
  "Return the json string representation of avro SCHEMA.

If CANONICAL-FORM is true, then return the Canonical Form as defined
in the avro spec."
  (let ((*schema->name* (make-hash-table :test #'eq))
        (*namespace* nil)
        (st-json:*output-literal-unicode*
          (or canonical-form
              st-json:*output-literal-unicode*)))
    (declare (special *schema->name* *namespace*))
    (st-json:write-json-to-string
     (%schema->jso (if canonical-form
                       (canonicalize schema)
                       schema)))))


(defgeneric %schema->jso (schema)
  (:method :before ((schema named-schema))
    (declare (special *namespace*)
             (optimize (speed 3) (safety 0)))
    (let ((fullname (fullname schema *namespace*)))
      (declare (special *schema->name*))
      (setf (gethash schema *schema->name*) fullname)))

  ;; to prevent infinite recursion when writing recursive schemas
  (:method :around (schema)
    (declare (special *schema->name*)
             (optimize (speed 3) (safety 0)))
    (let ((fullname (gethash schema *schema->name*)))
      (or fullname (call-next-method)))))

(macrolet
    ((defprimitives ()
       (flet ((make-defmethod (schema)
                `(defmethod %schema->jso ((schema (eql ',schema)))
                   (declare (ignore schema)
                            (optimize (speed 3) (safety 0)))
                   ,(schema->name schema))))
         `(progn
            ,@(mapcar #'make-defmethod *primitives*))))
     (defaliases ()
       (flet ((make-defmethod (cons)
                (destructuring-bind (logical . underlying)
                    cons
                  `(defmethod %schema->jso ((schema (eql ',logical)))
                     (declare (ignore schema)
                              (optimize (speed 3) (safety 0)))
                     (st-json:jso
                      "type" ,(schema->name underlying)
                      "logicalType" ,(schema->name logical))))))
         `(progn
            ,@(mapcar #'make-defmethod *logical-aliases*)))))
  (defprimitives)
  (defaliases))

(defunc fields->readers (schema-type fields)
  (declare (symbol schema-type))
  (unless (every #'symbolp fields)
    (error "Expected only symbols: ~S" fields))
  (flet ((make-reader (field)
           (intern (format nil "~S-~S" schema-type field))))
    (mapcar #'make-reader fields)))

(defmacro namespace->st-json (reader-form)
  (declare (cons reader-form))
  (let ((namespace (gensym)))
    `(let ((,namespace ,reader-form))
       (or ,namespace :null))))

(defmacro append-optionals (schema-type schema args &rest fields)
  "Append optional FIELDS to ARGS according to the corresponding predicate."
  (declare (symbol schema-type schema args))
  (let* ((readers (fields->readers schema-type fields))
         (predicates (mapcar #'+p readers))
         (json-fields (mapcar #'downcase-symbol fields))
         (when-forms (mapcar
                      (lambda (field reader predicate)
                        (let ((value (if (string= field "namespace")
                                         `(namespace->st-json (,reader ,schema))
                                         `(,reader ,schema))))
                          `(when (,predicate ,schema)
                             ;; need to maintain order for canonical form
                             (setf ,args (nconc ,args (list ,field ,value))))))
                      json-fields
                      readers
                      predicates)))
    `(progn
       ,@when-forms)))

;; order of required-fields matters for canonical form
(defmacro make-fields (schema-type schema (&rest required-fields) &rest optional-fields)
  (declare (symbol schema-type schema))
  (let* ((readers (fields->readers schema-type required-fields))
         (json-fields (mapcar #'downcase-symbol required-fields))
         (initial-fields (mapcan
                          (lambda (field reader)
                            `(,field (,reader ,schema)))
                          json-fields
                          readers))
         (type (list "type" (schema->name schema-type)))
         (fields (gensym)))
    ;; in canonical form: if name exists, then type is second
    ;; otherwise it's first
    (setf initial-fields
          (if (eq 'name (first required-fields))
              (nconc (subseq initial-fields 0 2) type (subseq initial-fields 2))
              (nconc type initial-fields)))
    `(let ((,fields (list ,@initial-fields)))
       (append-optionals ,schema-type ,schema ,fields ,@optional-fields)
       ,fields)))

(defmethod st-json:write-json-element ((vector simple-vector) stream)
  (declare (optimize (speed 3) (safety 0)))
  (let ((length (length vector)))
    (cond
      ((= 0 length)
       (write-string "[]" stream))
      ((= 1 length)
       (write-char #\[ stream)
       (st-json:write-json-element (svref vector 0) stream)
       (write-char #\] stream))
      (t
       (write-char #\[ stream)
       (st-json:write-json-element (svref vector 0) stream)
       (loop
         for i from 1 below length
         for elt = (svref vector i)
         do
            (write-char #\, stream)
            (st-json:write-json-element elt stream))
       (write-char #\] stream)))))

(defmethod %schema->jso ((schema fixed-schema))
  (declare (optimize (speed 3) (safety 0)))
  (apply #'st-json:jso (make-fields fixed-schema schema (name size) namespace aliases)))

(defmethod %schema->jso ((schema union-schema))
  (declare (optimize (speed 3) (safety 0)))
  (map 'simple-vector #'%schema->jso (union-schema-schemas schema)))

(defmethod %schema->jso ((schema array-schema))
  (declare (optimize (speed 3) (safety 0)))
  (st-json:jso
   "type" "array"
   "items" (%schema->jso (array-schema-items schema))))

(defmethod %schema->jso ((schema map-schema))
  (declare (optimize (speed 3) (safety 0)))
  (st-json:jso
   "type" "map"
   "values" (%schema->jso (map-schema-values schema))))

(defmethod %schema->jso ((schema enum-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((args (make-fields enum-schema schema (name symbols) namespace aliases doc))
        (default (enum-schema-default schema)))
    (when default
      (setf args (nconc args (list "default" default))))
    (apply #'st-json:jso args)))

(defmethod %schema->jso ((schema record-schema))
  (declare (special *namespace*)
           (optimize (speed 3) (safety 0)))
  (let ((args (make-fields record-schema schema (name) namespace aliases doc))
        (fields (let ((*namespace* (deduce-namespace (record-schema-name schema)
                                                     (record-schema-namespace schema)
                                                     *namespace*)))
                  (declare (special *namespace*))
                  (map 'simple-vector #'%field->jso (record-schema-fields schema)))))
    ;; fields come last in canonical form
    (apply #'st-json:jso (nconc args (list "fields" fields)))))

(defun+ %field->jso (schema)
    ((field-schema) st-json:jso)
  (let* ((type (field-schema-type schema))
         (args (list "name" (field-schema-name schema)
                     "type" (%schema->jso type)))
         (default (field-schema-default schema))
         (defaultp (field-schema-defaultp schema)))
    (append-optionals field-schema schema args aliases doc order)
    (when defaultp
      (let ((default (convert-to-st-json type default)))
        (setf args (nconc args (list "default" default)))))
    (apply #'st-json:jso args)))

(defmethod %schema->jso ((schema decimal-schema))
  (declare (optimize (speed 3) (safety 0)))
  (st-json:jso
   "type" (%schema->jso (decimal-schema-underlying-schema schema))
   "logicalType" "decimal"
   "precision" (decimal-schema-precision schema)
   "scale" (decimal-schema-scale schema)))

(defmethod %schema->jso ((schema duration-schema))
  (declare (optimize (speed 3) (safety 0)))
  (st-json:jso
   "type" (%schema->jso (duration-schema-underlying-schema schema))
   "logicalType" "duration"))

(defgeneric convert-to-st-json (schema value)
  (:method (schema value)
    (declare (ignore schema)
             (optimize (speed 3) (safety 0)))
    value)

  (:method ((schema (eql 'null-schema)) value)
    (declare (ignore schema value)
             (optimize (speed 3) (safety 0)))
    :null)

  (:method ((schema (eql 'boolean-schema)) boolean)
    (declare (ignore schema)
             (optimize (speed 3) (safety 0)))
    (st-json:as-json-bool boolean))

  (:method ((schema (eql 'bytes-schema)) (bytes simple-vector))
    (declare (ignore schema)
             (optimize (speed 3) (safety 0)))
    (babel:octets-to-string bytes :encoding :latin-1))

  (:method ((schema fixed-schema) (bytes simple-vector))
    (declare (ignore schema)
             (optimize (speed 3) (safety 0)))
    (babel:octets-to-string bytes :encoding :latin-1))

  (:method ((schema array-schema) (array simple-vector))
    (declare (optimize (speed 3) (safety 0)))
    (let ((item-schema (array-schema-items schema)))
      (flet ((convert (elt)
               (convert-to-st-json item-schema elt)))
        (map 'simple-vector #'convert array))))

  (:method ((schema map-schema) (map hash-table))
    (declare (optimize (speed 3) (safety 0)))
    (let ((hash-table (make-hash-table :test #'equal :size (hash-table-size map)))
          (value-schema (map-schema-values schema)))
      (flet ((convert (key value)
               (setf (gethash key hash-table)
                     (convert-to-st-json value-schema value))))
        (maphash #'convert map))
      hash-table))

  (:method ((schema record-schema) (record hash-table))
    (declare (optimize (speed 3) (safety 0)))
    (loop
      with fields = (record-schema-fields schema)
      with hash-table = (make-hash-table :test #'equal :size (length fields))

      for field across fields
      for name = (field-schema-name field)
      for type = (field-schema-type field)
      do (setf (gethash name hash-table)
               (convert-to-st-json type (gethash name record)))

      finally (return hash-table)))

  (:method ((schema union-schema) value)
    (declare (optimize (speed 3) (safety 0)))
    (let ((first-schema (svref (union-schema-schemas schema) 0)))
      (convert-to-st-json first-schema value))))
