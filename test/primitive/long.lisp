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
(defpackage #:cl-avro/test/long
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:import-from #:cl-avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/long)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "long"})
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= "\"long\"" (avro:serialize 'avro:long)))
    (is (= fingerprint (avro:fingerprint 'avro:long)))))

(test canonical-form
  (let ((json "\"long\"")
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:long :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:long)))))

(macrolet
    ((make-tests (&rest long->serialized)
       `(progn
          ,@(mapcar
             (lambda (long&serialized)
               `(define-io-test
                    ,(intern (format nil "IO-~A" (car long&serialized)))
                    ()
                    ,(if (typep (car long&serialized) '(signed-byte 32))
                         '(avro:long avro:int)
                         'avro:long)
                    ,(car long&serialized)
                    ,(cdr long&serialized)
                  (is (= object arg))))
             long->serialized))))
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
    avro:long
    (- (expt 2 63))
    (#xff #xff #xff #xff #xff #xff #xff #xff #xff #x01)
  (is (= object arg))
  (is (not (typep (1- arg) 'avro:long)))
  (signals error
    (avro:serialize (1- arg))))

(define-io-test io-max
    ()
    avro:long
    (1- (expt 2 63))
    (#xfe #xff #xff #xff #xff #xff #xff #xff #xff #x01)
  (is (= object arg))
  (is (not (typep (1+ arg) 'avro:long)))
  (signals error
    (avro:serialize (1+ arg))))
