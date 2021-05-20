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

(defpackage #:test/boolean
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax))

(in-package #:test/boolean)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "boolean"})
        (fingerprint #x9f42fc78a4d4f764))
    (is (eq 'avro:boolean (avro:deserialize 'avro:schema json)))
    (is (string= "\"boolean\"" (avro:serialize 'avro:boolean)))
    (is (= fingerprint (avro:fingerprint64 'avro:boolean)))))

(test canonical-form
  (let ((json "\"boolean\"")
        (fingerprint #x9f42fc78a4d4f764))
    (is (eq 'avro:boolean (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:boolean :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:boolean)))))

(test io-true
  (is (typep 'avro:true 'avro:boolean))
  (let ((serialized
          (make-array 1 :element-type '(unsigned-byte 8) :initial-element 1)))
    (is (equalp serialized (avro:serialize 'avro:true)))
    (is (eq 'avro:true (avro:deserialize 'avro:boolean serialized)))))

(test io-false
  (is (typep 'avro:false 'avro:boolean))
  (let ((serialized
          (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)))
    (is (equalp serialized (avro:serialize 'avro:false)))
    (is (eq 'avro:false (avro:deserialize 'avro:boolean serialized)))))
