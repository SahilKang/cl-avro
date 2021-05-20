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
                #:json-syntax))

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

(test io
  (loop
    with int->serialized
      = '((0 . (#x00))
          (-1 . (#x01))
          (1 . (#x02))
          (-2 . (#x03))
          (2 . (#x04))
          (-64 . (#x7f))
          (64 . (#x80 #x01))
          (8192 . (#x80 #x80 #x01))
          (-8193 . (#x81 #x80 #x01)))
        initially
           (dolist (cons int->serialized)
             (let* ((bytes-list (cdr cons))
                    (bytes-vector (make-array
                                   (length bytes-list)
                                   :element-type '(unsigned-byte 8)
                                   :initial-contents bytes-list)))
               (rplacd cons bytes-vector)))

    for (int . serialized) in int->serialized
    do
       (is (equalp serialized (avro:serialize int)))
       (is (= int (avro:deserialize 'avro:int serialized)))))

(test io-edges
  (let ((max (1- (expt 2 31)))
        (min (- (expt 2 31))))
    (is (typep max 'avro:int))
    (is (typep min 'avro:int))

    (is (not (typep (1+ max) 'avro:int)))
    (is (not (typep (1- min) 'avro:int)))

    (is (= max (avro:deserialize 'avro:int (avro:serialize max))))
    (is (= min (avro:deserialize 'avro:int (avro:serialize min))))

    ;; these get serialized as longs so that's why we check for an
    ;; error during deserialization
    (let ((serialized (avro:serialize (1+ max))))
      (signals error
        (avro:deserialize 'avro:int serialized)))
    (let ((serialized (avro:serialize (1- min))))
      (signals error
        (avro:deserialize 'avro:int serialized)))))
