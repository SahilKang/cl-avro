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

;;; good old-fashioned, gmo-free, grass-fed recursive-descent parser
;;; handcrafted with love in San Francisco

(defmacro defun* (name (&rest args) ((&rest arg-types) result-type) &body body)
  (declare (symbol name))
  (let ((declarations (rip-out-declarations body)))
    (push '(optimize (speed 3) (safety 3)) declarations)
    `(defun+ ,name ,args
         (,arg-types ,result-type)
       (declare ,@declarations)
       ,@body)))

(defmacro make-fullname->schema ()
  (let ((hash-table (gensym)))
    `(let ((,hash-table (make-hash-table :test #'equal)))
       (setf ,@(mapcan
                (lambda (schema)
                  `((gethash ,(schema->name schema) ,hash-table) ',schema))
                *primitives*))
       ,hash-table)))

(defun* json->schema (json)
    (((or simple-string stream)) avro-schema)
  "Return an avro schema according to JSON. JSON is a string or stream."
  (let ((*fullname->schema* (make-fullname->schema))
        (*namespace* nil))
    (declare (special *fullname->schema* *namespace*))
    (parse-schema (st-json:read-json json))))

(defun* parse-schema (json)
    (((or list simple-string st-json:jso)) avro-schema)
  (etypecase json
    (list (parse-union json))
    (simple-string (parse-string json))
    (st-json:jso (parse-jso json))))

(defun+ parse-string (fullname)
    ((simple-string) avro-schema)
  (declare (special *fullname->schema* *namespace*))
  (or (gethash fullname *fullname->schema*)
      (gethash (deduce-fullname fullname nil *namespace*) *fullname->schema*)
      (error "Unknown schema with fullname: ~S" fullname)))

(defun+ parse-union (union)
    ((list) union-schema)
  (make-union-schema :schemas (map 'simple-vector #'parse-schema union)))

(defmacro string-case (key &body cases)
  (declare (symbol key))
  (flet ((make-clause (case)
           (destructuring-bind (pattern &rest body)
               case
             (if (simple-string-p pattern)
                 `((string= ,key ,pattern) ,@body)
                 case))))
    (let ((clauses (mapcar #'make-clause cases)))
      `(cond
         ,@clauses))))

(defun+ %parse-jso (jso)
    ((st-json:jso) avro-schema)
  (let ((type (st-json:getjso "type" jso)))
    (check-type type simple-string)
    (string-case type
      ("record" (parse-record jso))
      ("enum" (parse-enum jso))
      ("array" (parse-array jso))
      ("map" (parse-map jso))
      ("fixed" (parse-fixed jso))
      (t (parse-string type)))))

(defmacro logical-case (logical-type underlying-schema &body cases)
  (declare (symbol logical-type underlying-schema))
  (flet ((make-clause (case)
           (destructuring-bind (pattern &rest body)
               case
             (typecase pattern
               (simple-string `((string= ,logical-type ,pattern) ,@body))
               (cons `((and (string= ,logical-type ,(first pattern))
                            (eq ,underlying-schema ',(second pattern)))
                       ,@body))
               (t case)))))
    (let ((clauses (mapcar #'make-clause cases)))
      `(cond
         ,@clauses))))

(defun+ parse-logical (logical-type jso)
    ((simple-string st-json:jso) avro-schema)
  (let ((underlying-schema (parse-schema (st-json:getjso "type" jso))))
    (handler-case
        (logical-case logical-type underlying-schema
          ("decimal" (parse-decimal underlying-schema jso))
          ("duration" (make-duration-schema :underlying-schema underlying-schema))
          (("uuid" string-schema) 'uuid-schema)
          (("date" int-schema) 'date-schema)
          (("time-millis" int-schema) 'time-millis-schema)
          (("time-micros" long-schema) 'time-micros-schema)
          (("timestamp-millis" long-schema) 'timestamp-millis-schema)
          (("timestamp-micros" long-schema) 'timestamp-micros-schema)
          (("local-timestamp-millis" long-schema) 'local-timestamp-millis-schema)
          (("local-timestamp-micros" long-schema) 'local-timestamp-micros-schema)
          (t underlying-schema))
      (error ()
        underlying-schema))))

(defmacro with-fields ((&rest fields) jso &body body)
  "Binds FIELDS as well as its elements suffixed with 'p'."
  (let* ((j (gensym))
         (fieldsp (mapcar (lambda (field) (+p field)) fields))
         (mvb-forms (mapcar
                     (lambda (field fieldp)
                       (let ((value (gensym))
                             (valuep (gensym)))
                         `(multiple-value-bind (,value ,valuep)
                              (st-json:getjso ,(downcase-symbol field) ,j)
                            (setf ,field ,value
                                  ,fieldp ,valuep))))
                     fields
                     fieldsp)))
    `(let ((,j ,jso)
           ,@fields
           ,@fieldsp)
       (declare (st-json:jso ,j))
       ,@mvb-forms
       ,@body)))

(defmacro make-kwargs (&rest symbols)
  "Each symbol should exactly match with field/slot, and p suffixes should exist."
  (flet ((->keyword (symbol)
           (intern (symbol-name symbol) 'keyword)))
    (let* ((args (gensym))
           (when-forms (mapcar
                        (lambda (symbol)
                          `(when ,(+p symbol)
                             (setf ,symbol (convert-from-st-json ,symbol))
                             (push ,symbol ,args)
                             (push ,(->keyword symbol) ,args)))
                        symbols)))
      `(let (,args)
         ,@when-forms
         ,args))))

(defun+ parse-decimal (underlying-schema jso)
    ((avro-schema st-json:jso) decimal-schema)
  (with-fields (precision scale) jso
    (let ((args (make-kwargs precision scale)))
      (push underlying-schema args)
      (push :underlying-schema args)
      (apply #'make-decimal-schema args))))

(defun+ parse-jso (jso)
    ((st-json:jso) avro-schema)
  (let ((logical-type (st-json:getjso "logicalType" jso)))
    (if (simple-string-p logical-type)
        (parse-logical logical-type jso)
        (%parse-jso jso))))

(defgeneric convert-from-st-json (value)
  (:method (value)
    (declare (optimize (speed 3) (safety 0)))
    value)

  (:method ((value (eql :null)))
    (declare (ignore value)
             (optimize (speed 3) (safety 0)))
    nil)

  (:method ((value (eql :true)))
    (declare (ignore value)
             (optimize (speed 3) (safety 0)))
    t)

  (:method ((value (eql :false)))
    (declare (ignore value)
             (optimize (speed 3) (safety 0)))
    nil)

  (:method ((list list))
    (declare (optimize (speed 3) (safety 0)))
    (map 'simple-vector #'convert-from-st-json list))

  (:method ((jso st-json:jso))
    (declare (optimize (speed 3) (safety 0)))
    (let ((hash-table (make-hash-table :test #'equal)))
      (flet ((convert (key value)
               (setf (gethash key hash-table)
                     (convert-from-st-json value))))
        (st-json:mapjso #'convert jso))
      hash-table)))

(defmacro with-args ((&rest symbols) jso &body body)
  "Binds an ARGS symbol for use in BODY."
  `(with-fields (,@symbols) ,jso
     (let ((args (make-kwargs ,@symbols)))
       ,@body)))

(defun* %register-named-schema (name namespace constructor args)
    ((avro-fullname avro-namespace function list) (values))
  (declare (special *fullname->schema* *namespace*))
  (let ((fullname (deduce-fullname name namespace *namespace*)))
    (when (nth-value 1 (gethash fullname *fullname->schema*))
      (error "Name ~S is already taken" fullname))
    (setf (gethash fullname *fullname->schema*)
          (apply constructor args))))

(defmacro register-named-schema (schema-type)
  (declare (symbol schema-type))
  (multiple-value-bind (constructor status)
      (find-symbol (format nil "MAKE-~S" schema-type))
    (unless (eq status :external)
      (error "Constructor for ~S is not external" schema-type))
    `(%register-named-schema name namespace #',constructor args)))

(defun+ parse-enum (jso)
    ((st-json:jso) enum-schema)
  (with-args (name namespace aliases doc symbols default) jso
    (register-named-schema enum-schema)))

(defun+ parse-fixed (jso)
    ((st-json:jso) fixed-schema)
  (with-args (name namespace aliases size) jso
    (register-named-schema fixed-schema)))

(defun+ parse-array (jso)
    ((st-json:jso) array-schema)
  (with-fields (items) jso
    (make-array-schema :items (parse-schema items))))

(defun+ parse-map (jso)
    ((st-json:jso) map-schema)
  (with-fields (values) jso
    (make-map-schema :values (parse-schema values))))

(defun+ parse-record (jso)
    ((st-json:jso) record-schema)
  (declare (special *namespace*))
  (with-fields (name namespace doc aliases fields) jso
    ;; record schemas can be recursive (fields can refer to the record
    ;; type), so let's register the record schema and then mutate the
    ;; fields vector
    (let ((args (make-kwargs name namespace doc aliases))
          (field-schemas (map 'simple-vector
                              (lambda (jso)
                                (%make-field-schema
                                 :name (st-json:getjso "name" jso)
                                 :type 'null-schema))
                              fields)))
      (push field-schemas args)
      (push :fields args)
      (let ((record (register-named-schema record-schema))
            (*namespace* (deduce-namespace name namespace *namespace*)))
        (declare (special *namespace*))
        (loop
          for i below (length field-schemas)
          for field = (parse-field (pop fields))
          do (setf (svref field-schemas i) field)

          finally (return record))))))

(defun* parse-field (jso)
     ((st-json:jso) (values field-schema &optional))
  (with-fields (name doc type order aliases default) jso
    (let ((args (make-kwargs name doc order aliases))
          (type (parse-schema type)))
      (push type args)
      (push :type args)
      (when defaultp
        (push (convert-from-st-json default) args)
        (push :default args))
      (apply #'make-field-schema args))))
