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

(defpackage #:test/bytes
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax))

(in-package #:test/bytes)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "bytes"})
        (fingerprint #x4fc016dac3201965))
    (is (eq 'avro:bytes (avro:deserialize 'avro:schema json)))
    (is (string= "\"bytes\"" (avro:serialize 'avro:bytes)))
    (is (= fingerprint (avro:fingerprint64 'avro:bytes)))))

(test canonical-form
  (let ((json "\"bytes\"")
        (fingerprint #x4fc016dac3201965))
    (is (eq 'avro:bytes (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:bytes :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:bytes)))))

(test io
  (let ((bytes
          (make-array 3 :element-type '(unsigned-byte 8)
                        :adjustable t :fill-pointer t
                        :initial-contents '(2 4 6)))
        (serialized
          (make-array 4 :element-type '(unsigned-byte 8)
                        :initial-contents '(6 2 4 6))))
    (is (typep bytes 'avro:bytes))
    (is (equalp serialized (avro:serialize bytes)))
    (is (equalp bytes (avro:deserialize 'avro:bytes serialized)))))

(test io-empty
  (let ((bytes
          (make-array 0 :element-type '(unsigned-byte 8)
                        :adjustable t :fill-pointer t))
        (serialized
          (make-array 1 :element-type '(unsigned-byte 8)
                        :initial-element 0)))
    (is (typep bytes 'avro:bytes))
    (is (equalp serialized (avro:serialize bytes)))
    (is (equalp bytes (avro:deserialize 'avro:bytes serialized)))))
