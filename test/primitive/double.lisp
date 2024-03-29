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
(defpackage #:cl-avro/test/double
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/double)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "double"})
        (fingerprint #x8e7535c032ab957e))
    (is (eq 'avro:double (avro:deserialize 'avro:schema json)))
    (is (string= "\"double\"" (avro:serialize 'avro:double)))
    (is (= fingerprint (avro:fingerprint 'avro:double)))))

(test canonical-form
  (let ((json "\"double\"")
        (fingerprint #x8e7535c032ab957e))
    (is (eq 'avro:double (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:double :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:double)))))

(define-io-test io
    ()
    avro:double
    23.7d0
    (#x33 #x33 #x33 #x33 #x33 #xB3 #x37 #x40)
  (is (= object arg)))

(define-io-test io-zero
    ()
    avro:double
    0.0d0
    (0 0 0 0 0 0 0 0)
  (is (= object arg)))
