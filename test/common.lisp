;;; Copyright 2021 Google LLC
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

(defpackage #:test/common
  (:use #:cl)
  (:export #:field
           #:json=
           #:json-string=
           #:define-schema-test
           #:define-io-test
           #:json-syntax))

(in-package #:test/common)

;;; field

(declaim
 (ftype (function (avro:record-object simple-string)
                  (values avro:object &optional))
        field))
(defun field (record field)
  (let ((found-field (find field (avro:fields (class-of record))
                           :key #'avro:name :test #'string=)))
    (unless found-field
      (error "No such field ~S" field))
    (slot-value record (nth-value 1 (avro:name found-field)))))

;;; json=

(declaim
 (ftype (function (st-json:jso st-json:jso) (values boolean &optional))
        same-fields-p))
(defun same-fields-p (lhs rhs)
  (flet ((fields (jso)
           (let (fields)
             (flet ((fill-fields (field value)
                      (declare (ignore value))
                      (push field fields)))
               (st-json:mapjso #'fill-fields jso))
             fields)))
    (let ((lhs (fields lhs))
          (rhs (fields rhs)))
      (null (nset-difference lhs rhs :test #'string=)))))

(declaim
 (ftype (function (st-json:jso st-json:jso) (values boolean &optional)) jso=))
(defun jso= (lhs rhs)
  (when (same-fields-p lhs rhs)
    (st-json:mapjso (lambda (field lhs)
                      (let ((rhs (st-json:getjso field rhs)))
                        (unless (json= lhs rhs)
                          (return-from jso= nil))))
                    lhs)
    t))

(declaim (ftype (function (t t) (values boolean &optional)) json=))
(defun json= (lhs rhs)
  (when (equal (type-of lhs) (type-of rhs))
    (typecase lhs
      (st-json:jso (jso= lhs rhs))
      (cons (and (= (length lhs) (length rhs))
                 (every #'json= lhs rhs)))
      (t (equal lhs rhs)))))

(declaim
 (ftype (function (string string) (values boolean &optional)) json-string=))
(defun json-string= (lhs rhs)
  (apply #'json= (mapcar #'st-json:read-json (list lhs rhs))))

;;; define-schema-test

(defmacro define-schema-test
    (name json canonical-form fingerprint make-instance-schema &rest defclass-schema)
  (declare (symbol name)
           (string json canonical-form)
           ((unsigned-byte 64) fingerprint)
           (cons defclass-schema)
           (cons make-instance-schema))
  `(1am:test ,name
     (let* ((expected-json ,json)
            (canonical-form ,canonical-form)
            (expected-fingerprint ,fingerprint)
            (make-instance-schema ,make-instance-schema)
            (defclass-schema (progn ,@defclass-schema))
            (expected-jso (st-json:read-json expected-json))
            (actual-schema (avro:deserialize 'avro:schema expected-json)))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize defclass-schema))))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize make-instance-schema))))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize actual-schema))))

       (1am:is (string= canonical-form (avro:serialize
                                        defclass-schema :canonical-form-p t)))
       (1am:is (string= canonical-form (avro:serialize
                                        make-instance-schema :canonical-form-p t)))
       (1am:is (string= canonical-form (avro:serialize
                                        actual-schema :canonical-form-p t)))

       (1am:is (= expected-fingerprint (avro:fingerprint defclass-schema)))
       (1am:is (= expected-fingerprint (avro:fingerprint make-instance-schema)))
       (1am:is (= expected-fingerprint (avro:fingerprint actual-schema))))))

;;; define-io-test

(eval-when (:compile-toplevel)
  (defun schema-pair-p (schema)
    (declare ((or symbol cons) schema))
    (and (consp schema)
         (not (member (first schema) '(make-instance closer-mop:ensure-class))))))

(defmacro define-io-test
    (name (&rest context) schema object (&rest serialized) &body check)
  (declare (symbol name))
  (flet ((assert-binding (binding)
           (etypecase binding
             (cons
              (unless (= (length binding) 2)
                (error "Expected a binding pair ~S" binding))
              (check-type (first binding) symbol))
             (symbol))))
    (map nil #'assert-binding context))
  (let ((schema-symbol (intern "SCHEMA"))
        (object-symbol (intern "OBJECT"))
        (arg (intern "ARG"))
        (schema (if (schema-pair-p schema) (first schema) schema))
        (schema-to-check (when (schema-pair-p schema) (second schema))))
    `(1am:test ,name
       (let* (,@context
              (,schema-symbol ,(let ((schema schema))
                                 (if (symbolp schema)
                                     (if (typep schema 'avro:schema)
                                         `',schema
                                         (find-class schema))
                                     schema)))
              (,object-symbol ,object)
              (serialized (make-array ,(length serialized)
                                      :element-type '(unsigned-byte 8)
                                      :initial-contents ',serialized))
              (schema-to-check ,(if schema-to-check
                                    (if (symbolp schema-to-check)
                                        (if (typep schema-to-check 'avro:schema)
                                            `',schema-to-check
                                            (find-class schema-to-check))
                                        schema-to-check)
                                    schema-symbol)))
         (flet ((check (,arg)
                  (declare (ignorable ,arg))
                  ,@check))
           (1am:is (eq schema-to-check (avro:schema-of ,object-symbol)))
           (check ,object-symbol)

           (multiple-value-bind (bytes size)
               (avro:serialize ,object-symbol)
             (1am:is (= (length serialized) size))
             (1am:is (equalp serialized bytes)))

           (let ((into (make-array (1+ (length serialized))
                                   :element-type '(unsigned-byte 8))))
             (multiple-value-bind (bytes size)
                 (avro:serialize ,object-symbol :into into :start 1)
               (1am:is (eq into bytes))
               (1am:is (= (length serialized) size))
               (1am:is (equalp serialized (subseq into 1))))
             (1am:signals error
               (avro:serialize ,object-symbol :into into :start 2)))

           (let ((bytes (flexi-streams:with-output-to-sequence (stream)
                          (multiple-value-bind (returned size)
                              (avro:serialize ,object-symbol :into stream)
                            (1am:is (eq stream returned))
                            (1am:is (= (length serialized) size))))))
             (1am:is (equalp serialized bytes)))

           (multiple-value-bind (deserialized size)
               (avro:deserialize ,schema-symbol serialized)
             (1am:is (= (length serialized) size))
             (1am:is (eq schema-to-check (avro:schema-of deserialized)))
             (check deserialized))

           (let ((input (make-array (1+ (length serialized))
                                    :element-type '(unsigned-byte 8))))
             (replace input serialized :start1 1)
             (multiple-value-bind (deserialized size)
                 (avro:deserialize ,schema-symbol input :start 1)
               (1am:is (= (length serialized) size))
               (1am:is (eq schema-to-check (avro:schema-of deserialized)))
               (check deserialized)))

           (multiple-value-bind (deserialized size)
               (flexi-streams:with-input-from-sequence (stream serialized)
                 (avro:deserialize ,schema-symbol stream))
             (1am:is (= (length serialized) size))
             (1am:is (eq schema-to-check (avro:schema-of deserialized)))
             (check deserialized)))))))

;;; json-syntax

(defun read-{ (stream char)
  (unread-char char stream)
  (st-json:write-json-to-string (st-json:read-json stream)))

(defun read-[ (stream char)
  (unread-char char stream)
  (st-json:write-json-to-string (st-json:read-json stream)))

(named-readtables:defreadtable json-syntax
  (:merge :standard)
  (:macro-char #\{ #'read-{)
  (:macro-char #\[ #'read-[))
