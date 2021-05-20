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

(defpackage #:test/string
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax))

(in-package #:test/string)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "string"})
        (fingerprint #x8f014872634503c7))
    (is (eq 'avro:string (avro:deserialize 'avro:schema json)))
    (is (string= "\"string\"" (avro:serialize 'avro:string)))
    (is (= fingerprint (avro:fingerprint64 'avro:string)))))

(test canonical-form
  (let ((json "\"string\"")
        (fingerprint #x8f014872634503c7))
    (is (eq 'avro:string (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:string :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:string)))))

(test io
  (let ((string
          (concatenate 'string (string #\u1f44b) " hello world!"))
        (serialized
          (make-array
           18
           :element-type '(unsigned-byte 8)
           :initial-contents
           '(34                            ; length
             #xf0 #x9f #x91 #x8b           ; waving hand sign emoji
             #x20 #x68 #x65 #x6c #x6c #x6f ; <space> hello
             #x20 #x77 #x6f #x72 #x6c #x64 #x21) ; <space> world!
           )))
    (is (typep string 'avro:string))
    (is (equalp serialized (avro:serialize string)))
    (is (string= string (avro:deserialize 'avro:string serialized)))))

(test io-empty
  (let ((string "")
        (serialized
          (make-array 1 :element-type '(unsigned-byte 8) :initial-element 0)))
    (is (typep string 'avro:string))
    (is (equalp serialized (avro:serialize string)))
    (is (string= string (avro:deserialize 'avro:string serialized)))))
