;;; Copyright 2021 Google LLC
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
(defpackage #:cl-avro/test/uuid
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))
(in-package #:cl-avro/test/uuid)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "string", "logicalType": "uuid"})
        (fingerprint #x33ec648d41dc37c9)
        (expected (find-class 'avro:uuid)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint expected)))))

(define-io-test io
    ((expected "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
    avro:uuid
    (make-instance 'avro:uuid :uuid expected)
    (72
     #x36 #x62 #x61 #x37 #x62 #x38 #x31 #x30 #x2d #x39 #x64 #x61 #x64
     #x2d #x31 #x31 #x64 #x31 #x2d #x38 #x30 #x62 #x34 #x2d #x30 #x30
     #x63 #x30 #x34 #x66 #x64 #x34 #x33 #x30 #x63 #x38)
  (is (string= expected (avro:raw arg)))
  (signals (or error warning)
    (make-instance 'avro:uuid :uuid "abc"))
  (signals (or error warning)
    (make-instance 'avro:uuid :uuid "")))

;; TODO move this into some generic logical fall-through
#+nil
(let ((schema (avro:deserialize
               'avro:schema "{type: \"bytes\", logicalType: \"uuid\"}")))
  (is (eq 'avro:bytes schema)))
