;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.io.read
  (:use #:cl)
  (:import-from #:cl-avro.schema.io.common
                #:+p
                #:downcase-symbol)
  (:import-from #:cl-avro.schema.io.st-json
                #:convert-from-st-json)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.complex
                #:schema
                #:named-schema
                #:fullname
                #:namespace
                #:record
                #:enum
                #:fixed)
  (:import-from #:cl-avro.schema.logical
                #:logical-schema
                #:underlying
                #:decimal
                #:duration)
  (:shadowing-import-from #:cl-avro.schema.complex
                          #:union
                          #:array
                          #:map)
  (:export #:json->schema))
(in-package #:cl-avro.schema.io.read)

(defmacro make-fullname->schema ()
  (let ((fullname->schema (gensym)))
    `(let ((,fullname->schema (make-hash-table :test #'equal)))
       (setf ,@(mapcan
                (lambda (pair)
                  (destructuring-bind (schema . name) pair
                    `((gethash ,name ,fullname->schema) ',schema)))
                +primitive->name+))
       ,fullname->schema)))

(declaim
 (ftype (function ((or string stream)) (values schema &optional))
        json->schema))
(defun json->schema (json)
  "Parse a schema from JSON."
  (let ((*fullname->schema* (make-fullname->schema))
        (*enclosing-namespace* nil)
        (*error-on-duplicate-name-p* t))
    (declare (special *fullname->schema* *enclosing-namespace*
                      *error-on-duplicate-name-p*))
    (parse-schema (st-json:read-json json))))

(declaim
 (ftype (function ((or list simple-string st-json:jso))
                  (values schema &optional))
        parse-schema))
(defun parse-schema (json)
  (etypecase json
    (list (parse-union json))
    (simple-string (parse-name json))
    (st-json:jso (parse-jso json))))

(declaim (ftype (function (list) (values union &optional)) parse-union))
(defun parse-union (union)
  (make-instance 'union :schemas (mapcar #'parse-schema union)))

(declaim
 (ftype (function (simple-string) (values schema &optional)) parse-name))
(defun parse-name (name)
  (declare (special *fullname->schema*))
  (or (nth-value 0 (gethash name *fullname->schema*))
      (parse-fullname name)
      (error "Unknown schema with name: ~S" name)))

(declaim
 (ftype (function (simple-string) (values (or null schema) &optional))
        parse-fullname))
(defun parse-fullname (name)
  (declare (special *fullname->schema* *enclosing-namespace*))
  (let* ((schema (make-instance 'named-schema
                                :name name
                                :enclosing-namespace *enclosing-namespace*))
         (fullname (fullname schema)))
    (nth-value 0 (gethash fullname *fullname->schema*))))

(declaim
 (ftype (function (st-json:jso) (values schema &optional)) parse-jso))
(defun parse-jso (jso)
  (let ((logical-type (st-json:getjso "logicalType" jso)))
    (if (simple-string-p logical-type)
        (parse-logical logical-type jso)
        (parse-complex jso))))

(declaim
 (ftype (function (simple-string st-json:jso) (values schema &optional))
        parse-logical))
(defun parse-logical (logical-type jso)
  (let ((underlying (parse-schema (st-json:getjso "type" jso))))
    (multiple-value-bind (logical status)
        (find-symbol (string-upcase logical-type) 'cl-avro.schema.logical)
      (if (eq status :external)
          (%parse-logical logical underlying jso)
          underlying))))

(declaim
 (ftype (function (symbol schema st-json:jso) (values schema &optional))
        %parse-logical))
(defun %parse-logical (logical underlying jso)
  (handler-case
      (case logical
        (decimal (parse-decimal underlying jso))
        (duration (make-instance 'duration :underlying underlying))
        (t (%%parse-logical (find-class logical) underlying)))
    (error ()
      underlying)))

(declaim
 (ftype (function (logical-schema schema) (values schema &optional))
        %%parse-logical))
(defun %%parse-logical (logical underlying)
  (if (eq (underlying logical) underlying)
      logical
      underlying))

(defmacro with-fields ((&rest fields) jso &body body)
  "Binds FIELDS as well as its elements suffixed with 'p'."
  (let* ((%jso (gensym))
         (fieldsp (mapcar #'+p fields))
         (mvb-forms (mapcar
                     (lambda (field fieldp)
                       (let ((value (gensym))
                             (valuep (gensym)))
                         `(multiple-value-bind (,value ,valuep)
                              (st-json:getjso ,(downcase-symbol field) ,%jso)
                            (setf ,field ,value
                                  ,fieldp ,valuep))))
                     fields
                     fieldsp)))
    `(let ((,%jso ,jso)
           ,@fields
           ,@fieldsp)
       (declare (st-json:jso ,%jso))
       ,@mvb-forms
       ,@body)))

(defmacro make-initargs (&rest bindings)
  "Each binding is either a symbol or (field initarg) list.

p suffixes for fields should exist."
  (labels
      ((keyword (symbol)
         (intern (symbol-name symbol) 'keyword))
       (parse-binding (binding)
         (if (symbolp binding)
             (list binding (+p binding) (keyword binding))
             (destructuring-bind (field initarg) binding
               (list field (+p field) (keyword initarg))))))
    (let* ((initargs (gensym))
           (when-forms (mapcar
                        (lambda (binding)
                          (destructuring-bind (field fieldp initarg)
                              (parse-binding binding)
                            `(when ,fieldp
                               (setf ,field (convert-from-st-json ,field))
                               (push ,field ,initargs)
                               (push ,initarg ,initargs))))
                        bindings)))
      `(let (,initargs)
         ,@when-forms
         ,initargs))))

(defmacro with-initargs ((&rest bindings) jso &body body)
  "Binds an INITARGS symbol for use in BODY."
  (flet ((parse-field (binding)
           (if (symbolp binding)
               binding
               (first binding))))
    (let ((fields (mapcar #'parse-field bindings)))
      `(with-fields (,@fields) ,jso
         (let ((initargs (make-initargs ,@bindings)))
           ,@body)))))

(declaim
 (ftype (function (schema st-json:jso) (values decimal &optional))
        parse-decimal))
(defun parse-decimal (underlying jso)
  (with-initargs (precision scale) jso
    (push underlying initargs)
    (push :underlying initargs)
    (apply #'make-instance 'decimal initargs)))

(declaim
 (ftype (function (st-json:jso) (values schema &optional)) parse-complex))
(defun parse-complex (jso)
  (let ((type (st-json:getjso "type" jso)))
    (check-type type simple-string)
    (let ((symbol (find-symbol (string-upcase type) 'cl-avro.schema.complex)))
      (case symbol
        (record (parse-record jso))
        (enum (parse-enum jso))
        (array (parse-array jso))
        (map (parse-map jso))
        (fixed (parse-fixed jso))
        (t (parse-name type))))))

(declaim
 (ftype (function (named-schema) (values named-schema &optional))
        register-named-schema))
(defun register-named-schema (schema)
  (declare (special *fullname->schema* *error-on-duplicate-name-p*))
  (let ((fullname (fullname schema)))
    (multiple-value-bind (registered-schema schema-registered-p)
        (gethash fullname *fullname->schema*)
      (if (not schema-registered-p)
          (setf (gethash fullname *fullname->schema*) schema)
          (if *error-on-duplicate-name-p*
              (error "Name ~S is already taken" fullname)
              registered-schema)))))

(declaim
 (ftype (function (named-schema) (values &optional)) unregister-named-schema))
(defun unregister-named-schema (schema)
  (declare (special *fullname->schema*))
  (let ((fullname (fullname schema)))
    (unless (remhash fullname *fullname->schema*)
      (error "~S does not name a schema" fullname)))
  (values))

(declaim (ftype (function (st-json:jso) (values enum &optional)) parse-enum))
(defun parse-enum (jso)
  (declare (special *enclosing-namespace*))
  (with-initargs
      (name namespace aliases (doc documentation) symbols default) jso
    (push *enclosing-namespace* initargs)
    (push :enclosing-namespace initargs)
    (register-named-schema
     (apply #'make-instance 'enum initargs))))

(declaim (ftype (function (st-json:jso) (values fixed &optional)) parse-fixed))
(defun parse-fixed (jso)
  (declare (special *enclosing-namespace*))
  (with-initargs (name namespace aliases size) jso
    (push *enclosing-namespace* initargs)
    (push :enclosing-namespace initargs)
    (register-named-schema
     (apply #'make-instance 'fixed initargs))))

(declaim (ftype (function (st-json:jso) (values array &optional)) parse-array))
(defun parse-array (jso)
  (with-fields (items) jso
    (if itemsp
        (make-instance 'array :items (parse-schema items))
        (make-instance 'array))))

(declaim (ftype (function (st-json:jso) (values map &optional)) parse-map))
(defun parse-map (jso)
  (with-fields (values) jso
    (if valuesp
        (make-instance 'map :values (parse-schema values))
        (make-instance 'map))))

(declaim
 (ftype (function (st-json:jso) (values record &optional)) parse-record))
(defun parse-record (jso)
  (declare (special *enclosing-namespace*))
  (with-initargs (name namespace aliases (doc documentation)) jso
    (push *enclosing-namespace* initargs)
    (push :enclosing-namespace initargs)
    (push (parse-fields name namespace jso) initargs)
    (push :direct-slots initargs)
    (let* ((schema (register-named-schema
                    (apply #'make-instance 'record initargs)))
           (fields (st-json:getjso "fields" jso))
           (*enclosing-namespace* (namespace schema))
           (*error-on-duplicate-name-p* nil))
      (declare (special *enclosing-namespace* *error-on-duplicate-name-p*))
      ;; it kinda sucks having *error-on-duplicate-name-p*
      ;; maybe just do an explicit dfs to get the new fields
      (reinitialize-instance
       schema :direct-slots (mapcar #'parse-field fields)))))

(declaim
 (ftype (function (fullname namespace st-json:jso) (values list &optional))
        parse-fields))
(defun parse-fields (name namespace jso)
  (declare (special *enclosing-namespace*))
  ;; using a placeholder because record schemas can be recursive:
  ;; fields can refer to the record being defined
  (let* ((placeholder (register-named-schema
                       (make-instance
                        'named-schema
                        :name name
                        :namespace namespace
                        :enclosing-namespace *enclosing-namespace*)))
         (*enclosing-namespace* (namespace placeholder)))
    (declare (special *enclosing-namespace*))
    (with-fields (fields) jso
      (unless (and fieldsp (listp fields))
        (error "Record schema must provide an array of fields."))
      (prog1 (mapcar #'parse-field fields)
        (unregister-named-schema placeholder)))))

(declaim (ftype (function (st-json:jso) (values list &optional)) parse-field))
(defun parse-field (jso)
  (with-initargs ((doc documentation) order aliases default) jso
    (let ((name (make-symbol (st-json:getjso "name" jso)))
          (type (parse-schema (st-json:getjso "type" jso))))
      (push name initargs)
      (push :name initargs)
      (push type initargs)
      (push :type initargs))
    initargs))
