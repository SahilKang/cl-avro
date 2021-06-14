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

(defpackage #:test/int
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax
                #:define-io-test))

(in-package #:test/int)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "int"})
        (fingerprint #x7275d51a3f395c8f))
    (is (eq 'avro:int (avro:deserialize 'avro:schema json)))
    (is (string= "\"int\"" (avro:serialize 'avro:int)))
    (is (= fingerprint (avro:fingerprint64 'avro:int)))))

(test canonical-form
  (let ((json "\"int\"")
        (fingerprint #x7275d51a3f395c8f))
    (is (eq 'avro:int (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:int :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:int)))))

(macrolet
    ((make-tests (&rest int->serialized)
       `(progn
          ,@(mapcar
             (lambda (int&serialized)
               `(define-io-test
                    ,(intern (format nil "IO-~A" (car int&serialized)))
                    ()
                    avro:int
                    ,(car int&serialized)
                    ,(cdr int&serialized)
                  (is (= object arg))))
             int->serialized))))
  (make-tests
   (0 . (#x00))
   (-1 . (#x01))
   (1 . (#x02))
   (-2 . (#x03))
   (2 . (#x04))
   (-64 . (#x7f))
   (64 . (#x80 #x01))
   (8192 . (#x80 #x80 #x01))
   (-8193 . (#x81 #x80 #x01))))

(define-io-test io-min
    ()
    avro:int
    (- (expt 2 31))
    (#xff #xff #xff #xff #x0f)
  (is (= object arg))
  (is (not (typep (1- arg) 'avro:int)))
  ;; this gets serialized as a long so that's why we check for an
  ;; error during deserialization
  (signals error
    (avro:deserialize 'avro:int (avro:serialize (1- arg)))))

(define-io-test io-max
    ()
    avro:int
    (1- (expt 2 31))
    (#xfe #xff #xff #xff #x0f)
  (is (= object arg))
  (is (not (typep (1+ arg) 'avro:int)))
  (signals error
    (avro:deserialize 'avro:int (avro:serialize (1+ arg)))))
