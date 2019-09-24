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

(in-package #:cl-user)

(defpackage #:test/parser
  (:use #:cl #:1am))

(in-package #:test/parser)

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
         (schema (avro:json->schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:schema->json schema))))
    (let ((valid '("foo" (7)))
          (invalid '("foo" ("abc"))))
      (is (avro:validp schema valid))
      (is (not (avro:validp schema invalid))))

    (let ((fields (avro:field-schemas schema)))
      (is (equalp nil (avro:default (elt fields 0))))
      (is (equalp #(4) (avro:default (elt fields 1)))))

    (let ((nested-field (second
                         (st-json:getjso "fields" roundtrip-jso))))
      (is (= 4 (st-json:getjso* "default.value" nested-field)))
      (is (same-fields-p jso roundtrip-jso)))))

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
         (schema (avro:json->schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:schema->json schema))))
    (let ((valid '(("hello" 8) ("world" 9)))
          (invalid '(("hello" 8) ("world" "!"))))
      (is (avro:validp schema valid))
      (is (not (avro:validp schema invalid))))

    (let ((fields (avro:field-schemas
                   (avro:item-schema schema))))
      (is (string= "foo" (avro:default (elt fields 0))))
      (is (= 7 (avro:default (elt fields 1)))))

    (let ((fields (st-json:getjso* "items.fields" roundtrip-jso)))
      (is (string= "foo" (st-json:getjso "default" (first fields))))
      (is (= 7 (st-json:getjso "default" (second fields))))
      (is (same-fields-p jso roundtrip-jso)))))

(test parse-map
  (let* ((json "{type: \"map\",
                 values: {type: \"enum\",
                          name: \"EnumName\",
                          default: \"BAR\",
                          symbols: [\"FOO\", \"BAR\", \"BAZ\"]}}")
         (schema (avro:json->schema json))
         (jso (st-json:read-json json))
         (roundtrip-jso (st-json:read-json
                         (avro:schema->json schema))))
    (let ((valid (make-hash-table :test #'equal))
          (invalid (make-hash-table :test #'equal)))
      (setf (gethash "key-1" valid) "FOO"
            (gethash "key-2" valid) "BAR"
            (gethash "key-3" valid) "BAZ"

            (gethash "key-1" invalid) "foo"
            (gethash "key-2" invalid) "BAR")
      (is (avro:validp schema valid))
      (is (not (avro:validp schema invalid))))

    (is (string= "BAR" (avro:default (avro:value-schema schema))))

    (is (string= "BAR" (st-json:getjso* "values.default" roundtrip-jso)))
    (is (same-fields-p jso roundtrip-jso))))
