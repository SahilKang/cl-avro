;;; Copyright 2022 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro/test/name
  (:local-nicknames
   (#:avro #:cl-avro))
  (:use #:cl #:1am))
(in-package #:cl-avro/test/name)

(test both
  (let* ((class-name (gensym))
         (name "foo")
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))))

(test class-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (schema (make-instance 'avro:fixed :name class-name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))))

(test none
  (signals error
    (make-instance 'avro:fixed :size 1)))

(test reinitialize-both
  (let* ((class-name (gensym))
         (name "foo")
         (other-name (format nil "~A_bar" name))
         (other-class-name (gensym))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance
                    schema :name other-class-name :name other-name :size 1)))
    (is (eq (class-name schema) other-class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-class-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (other-name (format nil "~A_bar" name))
         (other-class-name (make-symbol other-name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance
                    schema :name other-class-name :size 1)))
    (is (eq (class-name schema) other-class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-name-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (other-name (format nil "~A_bar" name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance schema :name other-name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-none
  (let* ((name "foo")
         (other-name (format nil "~A_bar" name))
         (class-name (make-symbol other-name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance schema :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) other-name))))
