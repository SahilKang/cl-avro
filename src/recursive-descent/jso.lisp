;;; Copyright 2021, 2023-2024 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.recursive-descent.jso
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:get-value
                #:pattern-generic-function)
  (:import-from #:cl-avro.internal.type
                #:ufixnum))
(in-package #:cl-avro.internal.recursive-descent.jso)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (symbol) (values simple-string &optional))
          internal:downcase-symbol))
  (defun internal:downcase-symbol (symbol)
    (string-downcase (symbol-name symbol))))

(defmethod st-json:write-json-element
    ((vector vector) (stream stream))
  (if (zerop (length vector))
      (write-string "[]" stream)
      (loop
        initially
           (write-char #\[ stream)
           (st-json:write-json-element (elt vector 0) stream)

        for index from 1 below (length vector)
        for element = (elt vector index)
        do
           (write-char #\, stream)
           (st-json:write-json-element element stream)

        finally
           (write-char #\] stream))))

;;; deserialize

(defgeneric internal:read-jso (jso fullname->schema enclosing-namespace)
  (:generic-function-class pattern-generic-function))

(defmethod get-value
    ((key string) (value st-json:jso))
  (st-json:getjso key value))

;; TODO return number of characters consumed
(defmethod api:deserialize
    ((schema (eql 'api:schema)) (input stream) &key)
  (declare (ignore schema))
  (let* ((jso (st-json:read-json input t))
         (fullname->schema (make-hash-table :test #'equal))
         (schema (internal:read-jso jso fullname->schema nil)))
    (closer-mop:ensure-finalized schema nil)
    schema))

(defmethod api:deserialize
    ((schema (eql 'api:schema)) (input string) &key (start 0))
  (declare (ignore schema))
  (multiple-value-bind (jso index)
      (st-json:read-json-from-string input :start start :junk-allowed-p t)
    (let* ((fullname->schema (make-hash-table :test #'equal))
           (schema (internal:read-jso jso fullname->schema nil)))
      (closer-mop:ensure-finalized schema nil)
      (values schema (- index start)))))

(defmethod api:deserialize
    ((schema symbol) input &rest initargs)
  "Deserialize with class named SCHEMA."
  (apply #'api:deserialize (find-class schema) input initargs))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (ftype (function (symbol) (values symbol &optional)) +p))
  (defun +p (name)
    (nth-value 0 (intern (format nil "~SP" name)))))

;;; with-initargs

(defmacro internal:with-initargs ((&rest bindings) jso &body body)
  "Binds an INITARGS symbol for use in BODY.

Each binding should either be a symbol or (field initarg) list."
  (flet ((parse-binding (binding)
           (if (symbolp binding)
               (values (internal:downcase-symbol binding)
                       (intern (string binding) 'keyword))
               (destructuring-bind (field initarg) binding
                 (declare (symbol field)
                          (keyword initarg))
                 (values (internal:downcase-symbol field) initarg)))))
    (let ((%jso (gensym))
          (initargs (intern "INITARGS")))
      `(let ((,%jso ,jso)
             (,initargs nil))
         ,@(mapcar (lambda (binding)
                     (multiple-value-bind (field initarg)
                         (parse-binding binding)
                       (let ((value (gensym))
                             (valuep (gensym)))
                         `(multiple-value-bind (,value ,valuep)
                              (st-json:getjso ,field ,%jso)
                            (when ,valuep
                              (push ,value ,initargs)
                              (push ,initarg ,initargs))))))
                   bindings)
         ,@body))))
