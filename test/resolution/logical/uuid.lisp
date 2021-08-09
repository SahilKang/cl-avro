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
(defpackage #:test/resolution/uuid
  (:use #:cl #:1am))
(in-package #:test/resolution/uuid)

(test uuid->uuid
  (let* ((writer (make-instance 'avro:uuid :uuid "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
         (reader (avro:deserialize
                  'avro:uuid
                  (avro:serialize writer)
                  :reader-schema 'avro:uuid)))
    (is (typep writer 'avro:uuid))
    (is (typep reader 'avro:uuid))
    (is (string= (avro:uuid writer) (avro:uuid reader)))))

(test uuid->string
  (let* ((writer (make-instance 'avro:uuid :uuid "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
         (reader (avro:deserialize
                  'avro:uuid
                  (avro:serialize writer)
                  :reader-schema 'avro:string)))
    (is (typep writer 'avro:uuid))
    (is (typep reader 'avro:string))
    (is (string= (avro:uuid writer) reader))))

(test string->uuid
  (let* ((writer "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
         (reader (avro:deserialize
                  'avro:string
                  (avro:serialize writer)
                  :reader-schema 'avro:uuid)))
    (is (typep writer 'avro:string))
    (is (typep reader 'avro:uuid))
    (is (string= writer (avro:uuid reader)))))

(test uuid->bytes
  (let* ((writer (make-instance 'avro:uuid :uuid "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
         (reader (avro:deserialize
                  'avro:uuid
                  (avro:serialize writer)
                  :reader-schema 'avro:bytes)))
    (is (typep writer 'avro:uuid))
    (is (typep reader 'avro:bytes))
    (is (string= (avro:uuid writer) (babel:octets-to-string reader :encoding :utf-8)))))

(test bytes->uuid
  (let* ((writer (babel:string-to-octets
                  "6ba7b810-9dad-11d1-80b4-00c04fd430c8" :encoding :utf-8))
         (reader (avro:deserialize
                  'avro:bytes
                  (avro:serialize writer)
                  :reader-schema 'avro:uuid)))
    (is (typep writer 'avro:bytes))
    (is (typep reader 'avro:uuid))
    (is (string= (babel:octets-to-string writer :encoding :utf-8) (avro:uuid reader)))))
