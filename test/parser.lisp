;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:test/parser
  (:use #:cl #:1am))

(in-package #:test/parser)

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


(defun jso-fields (jso)
  "Returns all fields contained in JSO.

Nested fields are delimited with a dot and array elements are suffixed with
their offset.

Example: '(\"abc.d[0].e\", \"abc.d[1].e\")."
  (let ((fields nil)
        (*parents* nil))
    (declare (special *parents*))
    (labels ((fully-qualify (field)
               "Return the fully qualified path for FIELD."
               (reduce (lambda (x y)
                         (format nil "~A.~A" x y))
                       *parents*
                       :initial-value field
                       :from-end t))

             (recursive-step (k v &optional offset)
               (let ((*parents* *parents*))
                 (declare (special *parents*))
                 (when offset
                   (let ((last
                          (format nil "~A[~A]" (car (last *parents*)) offset)))
                     (setf *parents* (nconc (butlast *parents*) (list last)))))
                 (setf fields (nconc fields (list (fully-qualify k))))
                 (let ((*parents* (append *parents* (list k))))
                   (declare (special *parents*))
                   (typecase v
                     (list (loop
                              for elt in v and i from 0
                              when (typep elt 'st-json:jso)
                              do (st-json:mapjso (lambda (k v)
                                                   (recursive-step k v i))
                                                 elt)))
                     (st-json:jso (st-json:mapjso #'recursive-step v)))))))
      (st-json:mapjso #'recursive-step jso))
    fields))

(defun same-fields-p (lhs rhs)
  "Determine if LHS and RHS jso objects have the same fields."
  (let ((lhs (jso-fields lhs))
        (rhs (jso-fields rhs)))
    (null (nset-difference lhs rhs :test #'string=))))

(test parse-record
  (let* ((json "{type: \"record\",
                 name: \"OuterRecord\",
                 fields: [
                   {name: \"name\",
                    type: \"string\"},
                   {name: \"nested\",
                    default: {value: 4},
                    type: {type: \"record\",
                           name: \"InnerRecord\",
                           fields: [
                             {name: \"value\",
                              type: \"int\"}
                           ]}}
                 ]}")
         (schema (avro:deserialize 'avro:schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:serialize schema))))
    (let ((fields (avro:fields schema)))
      (multiple-value-bind (default defaultp)
          (avro:default (elt fields 0))
        (is (eq nil defaultp))
        (is (eq nil default)))
      (multiple-value-bind (default defaultp)
          (avro:default (elt fields 1))
        (is (eq t defaultp))
        (is (= 4 (field default "value")))))

    (is (= 4 (st-json:getjso*
              "default.value"
              (second (st-json:getjso "fields" roundtrip-jso)))))

    (is (same-fields-p jso roundtrip-jso))))

(test parse-array
  (let* ((json "{type: \"array\",
                 items: {type: \"record\",
                         name: \"RecordName\",
                         fields: [
                           {name: \"Foo\",
                            type: \"string\",
                            default: \"foo\"},
                           {name: \"Bar\",
                            type: \"int\",
                            default: 7}
                         ]}}")
         (schema (avro:deserialize 'avro:schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:serialize schema))))
    (let ((fields (avro:fields (avro:items schema))))
      (multiple-value-bind (default defaultp)
          (avro:default (elt fields 0))
        (is (eq t defaultp))
        (is (string= "foo" default)))
      (multiple-value-bind (default defaultp)
          (avro:default (elt fields 1))
        (is (eq t defaultp))
        (is (= 7 default))))

    (let ((fields (st-json:getjso* "items.fields" roundtrip-jso)))
      (is (string= "foo" (st-json:getjso "default" (first fields))))
      (is (= 7 (st-json:getjso "default" (second fields)))))

    (is (same-fields-p jso roundtrip-jso))))

(test parse-map
  (let* ((json "{type: \"map\",
                 values: {type: \"enum\",
                          name: \"EnumName\",
                          default: \"BAR\",
                          symbols: [\"FOO\", \"BAR\", \"BAZ\"]}}")
         (schema (avro:deserialize 'avro:schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:serialize schema))))
    (multiple-value-bind (default position)
        (avro:default (avro:values schema))
      (is (string= "BAR" default))
      (is (= 1 position)))

    (is (string= "BAR" (st-json:getjso* "values.default" roundtrip-jso)))
    (is (same-fields-p jso roundtrip-jso))))

(test optional-fields
  (let* ((json "{type: \"enum\",
                 name: \"EnumName\",
                 aliases: [],
                 default: \"FOO\",
                 symbols: [\"FOO\", \"BAR\", \"BAZ\"]}")
         (schema (avro:deserialize 'avro:schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:serialize schema))))
    (is (same-fields-p jso roundtrip-jso))

    (is (equalp #() (avro:aliases schema)))
    (multiple-value-bind (aliases aliasesp)
        (st-json:getjso "aliases" roundtrip-jso)
      (is aliasesp)
      (is (null aliases)))

    (is (null (documentation schema t)))
    (multiple-value-bind (doc docp)
        (st-json:getjso "doc" roundtrip-jso)
      (is (not docp))
      (is (null doc)))

    (multiple-value-bind (default position)
        (avro:default schema)
      (is (string= "FOO" default))
      (is (= 0 position)))
    (multiple-value-bind (default defaultp)
        (st-json:getjso "default" roundtrip-jso)
      (is defaultp)
      (is (string= "FOO" default)))))

(test canonical-form
  (let ((schema (avro:deserialize
                 'avro:schema
                 "{type: \"record\",
                   name: \"OuterRecord\",
                   namespace: \"name.space\",
                   fields: [
                     {name: \"name\",
                      type: \"string\"},
                     {name: \"foo\",
                      default: {value: 4},
                      type: {type: \"record\",
                             name: \"InnerRecord\",
                             namespace: \"f\\u006fo.bar\",
                             fields: [
                               {name: \"value\",
                                type: \"int\"}
                             ]}}
                   ]}"))
        (expected (concatenate
                   'string
                   "{\"name\":\"name.space.OuterRecord\","
                   "\"type\":\"record\","
                   "\"fields\":["
                   "{\"name\":\"name\","
                   "\"type\":\"string\"},"
                   "{\"name\":\"foo\","
                   "\"type\":{"
                   "\"name\":\"foo.bar.InnerRecord\","
                   "\"type\":\"record\","
                   "\"fields\":["
                   "{\"name\":\"value\","
                   "\"type\":\"int\"}]}}]}")))
    (is (string= expected (avro:serialize schema :canonical-form-p t)))))
