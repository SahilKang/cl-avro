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

(defpackage #:test/float
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax))

(in-package #:test/float)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "float"})
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= "\"float\"" (avro:serialize 'avro:float)))
    (is (= fingerprint (avro:fingerprint64 'avro:float)))))

(test canonical-form
  (let ((json "\"float\"")
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:float :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:float)))))

(test io
  (let ((float 23.7f0)
        (serialized
          (make-array 4 :element-type '(unsigned-byte 8)
                        :initial-contents '(#x9a #x99 #xbd #x41))))
    (is (typep float 'avro:float))
    (is (equalp serialized (avro:serialize float)))
    (is (= float (avro:deserialize 'avro:float serialized)))))

(test io-zero
  (let ((float 0.0f0)
        (serialized
          (make-array 4 :element-type '(unsigned-byte 8) :initial-element 0)))
    (is (typep float 'avro:float))
    (is (equalp serialized (avro:serialize float)))
    (is (= float (avro:deserialize 'avro:float serialized)))))
