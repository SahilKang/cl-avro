;;; Copyright 2022 Google LLC
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
(defpackage #:cl-avro/test/reinitialization
  (:local-nicknames
   (#:avro #:cl-avro))
  (:use #:cl #:1am))
(in-package #:cl-avro/test/reinitialization)

(test direct
  (let ((schema (make-instance 'avro:fixed :name "foo" :size 1)))
    (is (= 1 (avro:size schema)))
    (is (eq schema (reinitialize-instance schema :name "foo" :size 2)))
    (is (= 2 (avro:size schema)))))

(test inherited
  (let ((schema (make-instance 'avro:fixed :name "bar" :namespace "foo" :size 1)))
    (is (string= "foo.bar" (avro:fullname schema)))
    (is (eq schema (reinitialize-instance schema :name "bar" :size 1)))
    (is (string= "bar" (avro:fullname schema)))))

(test invalid
  (let ((schema (make-instance 'avro:fixed :name "foo" :size 1)))
    (signals error
      (reinitialize-instance schema :name "foo"))))
