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

(defpackage #:test/null
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:define-io-test))

(in-package #:test/null)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "null"})
        (fingerprint #x63dd24e7cc258f8a))
    (is (eq 'avro:null (avro:deserialize 'avro:schema json)))
    (is (string= "\"null\"" (avro:serialize 'avro:null)))
    (is (= fingerprint (avro:fingerprint64 'avro:null)))))

(test canonical-form
  (let ((json "\"null\"")
        (fingerprint #x63dd24e7cc258f8a))
    (is (eq 'avro:null (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:null :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:null)))))

(define-io-test io
    ()
    avro:null
    nil
    ()
  (is (eq object arg))
  (multiple-value-bind (deserialized size)
      (let ((bytes (make-array 3 :element-type '(unsigned-byte 8)
                                 :initial-contents '(2 4 6))))
        (avro:deserialize 'avro:null bytes))
    (is (zerop size))
    (is (eq object deserialized))))
