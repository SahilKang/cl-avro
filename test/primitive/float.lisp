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
(defpackage #:cl-avro/test/float
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/float)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "float"})
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= "\"float\"" (avro:serialize 'avro:float)))
    (is (= fingerprint (avro:fingerprint 'avro:float)))))

(test canonical-form
  (let ((json "\"float\"")
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:float :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:float)))))

(define-io-test io
    ()
    avro:float
    23.7f0
    (#x9a #x99 #xbd #x41)
  (is (= object arg)))

(define-io-test io-zero
    ()
    avro:float
    0.0f0
    (0 0 0 0)
  (is (= object arg)))
