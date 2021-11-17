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
(defpackage #:cl-avro/test/resolution/uuid
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)))
(in-package #:cl-avro/test/resolution/uuid)

(test uuid->uuid
  (let* ((writer (make-instance 'avro:uuid :uuid "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
         (reader (avro:coerce
                  (avro:deserialize 'avro:uuid (avro:serialize writer))
                  'avro:uuid)))
    (is (typep writer 'avro:uuid))
    (is (typep reader 'avro:uuid))
    (is (string= (avro:raw writer) (avro:raw reader)))))
