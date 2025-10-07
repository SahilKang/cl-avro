;;; Copyright 2023 Google LLC
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
(defpackage #:cl-avro/test/intern
  (:local-nicknames
   (#:avro #:cl-avro))
  (:use #:cl #:1am))
(in-package #:cl-avro/test/intern)

(test fixed
  (when (find-package "foo")
    (delete-package "foo"))
  (let* ((schema
           (make-instance 'avro:fixed :namespace "foo" :name "bar" :size 2))
         (interned (avro:intern schema)))
    (multiple-value-bind (symbol status)
        (find-symbol "bar" (find-package "foo"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq schema (find-class interned)))))

(test enum
  (when (find-package "namespace")
    (delete-package "namespace"))
  (when (find-package "namespace.enum_name")
    (delete-package "namespace.enum_name"))
  (let* ((schema
           (make-instance 'avro:enum :namespace "namespace" :name "enum_name"
                                     :symbols '("FOO" "BAR" "BAZ")))
         (interned (avro:intern schema)))
    (multiple-value-bind (symbol status)
        (find-symbol "enum_name" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq schema (find-class interned)))
    (let ((package (find-package "namespace.enum_name")))
      (multiple-value-bind (symbol status)
          (find-symbol "FOO" package)
        (is (eq :external status))
        (is (string= "FOO" (avro:which-one (symbol-value symbol)))))
      (multiple-value-bind (symbol status)
          (find-symbol "BAR" package)
        (is (eq :external status))
        (is (string= "BAR" (avro:which-one (symbol-value symbol)))))
      (multiple-value-bind (symbol status)
          (find-symbol "BAZ" package)
        (is (eq :external status))
        (is (string= "BAZ" (avro:which-one (symbol-value symbol))))))))

(test record
  (when (find-package "namespace")
    (delete-package "namespace"))
  (let* ((schema
           (make-instance 'avro:record :namespace "namespace"
                                       :name '#:record_name
                                       :direct-slots
                                       '((:name #:foo
                                          :type avro:int
                                          :readers (#:read-foo)
                                          :writers ((setf #:write-foo))))))
         (interned (avro:intern schema))
         (package (find-package "namespace")))
    (multiple-value-bind (symbol status)
        (find-symbol "RECORD_NAME" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq schema (find-class interned)))
    (multiple-value-bind (symbol status)
        (find-symbol "READ-FOO" package)
      (is (eq :external status))
      (let ((specializers (mapcan #'closer-mop:method-specializers
                                  (closer-mop:generic-function-methods
                                   (fdefinition symbol)))))
        (is (find schema specializers))))
    (multiple-value-bind (symbol status)
        (find-symbol "WRITE-FOO" package)
      (is (eq :external status))
      (let ((specializers (mapcan #'closer-mop:method-specializers
                                  (closer-mop:generic-function-methods
                                   (fdefinition `(setf ,symbol))))))
        (is (find schema specializers))))))

(test record-add-methods
  (when (find-package "namespace")
    (delete-package "namespace"))
  (let* ((schema-1 (make-instance 'avro:record
                                  :namespace "namespace"
                                  :name '#:record_name_1
                                  :direct-slots
                                  '((:name #:foo
                                     :type avro:int
                                     :readers (#:read-foo)
                                     :writers ((setf #:write-foo))))))
         (schema-2 (make-instance 'avro:record
                                  :namespace "namespace"
                                  :name '#:record_name_2
                                  :direct-slots
                                  '((:name #:foo
                                     :type avro:int
                                     :readers (#:read-foo)
                                     :writers ((setf #:write-foo))))))
         (interned-1 (avro:intern schema-1))
         (interned-2 (avro:intern schema-2))
         (package (find-package "namespace")))
    (multiple-value-bind (symbol status)
        (find-symbol "RECORD_NAME_1" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned-1)))
    (multiple-value-bind (symbol status)
        (find-symbol "RECORD_NAME_2" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned-2)))
    (is (eq schema-1 (find-class interned-1)))
    (is (eq schema-2 (find-class interned-2)))
    (multiple-value-bind (symbol status)
        (find-symbol "READ-FOO" package)
      (is (eq :external status))
      (let ((specializers (mapcan #'closer-mop:method-specializers
                                  (closer-mop:generic-function-methods
                                   (fdefinition symbol)))))
        (is (find schema-1 specializers))
        (is (find schema-2 specializers))))
    (multiple-value-bind (symbol status)
        (find-symbol "WRITE-FOO" package)
      (is (eq :external status))
      (let ((specializers (mapcan #'closer-mop:method-specializers
                                  (closer-mop:generic-function-methods
                                   (fdefinition `(setf ,symbol))))))
        (is (find schema-1 specializers))
        (is (find schema-2 specializers))))))

(test protocol
  (when (find-package "namespace")
    (delete-package "namespace"))
  (let* ((errors (list
                  (cl-avro.internal.ipc.error::to-error
                   (make-instance 'avro:record
                                  :namespace "namespace"
                                  :name '#:record_name))))
         (protocol (make-instance 'avro:protocol
                                  :namespace "namespace"
                                  :name "protocol_name"
                                  :types errors
                                  :messages
                                  `((:name #:message_name
                                     :request ((:name #:greeting
                                                :type avro:string))
                                     :response avro:null
                                     :errors ,errors))))
         (interned (avro:intern protocol)))
    (multiple-value-bind (symbol status)
        (find-symbol "protocol_name" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq protocol (find-class interned)))
    (let ((package (find-package "namespace"))
          (function (elt (avro:messages protocol) 0)))
      (multiple-value-bind (symbol status)
          (find-symbol "MESSAGE_NAME" package)
        (is (eq :external status))
        (is (eq function (fdefinition symbol))))
      (multiple-value-bind (symbol status)
          (find-symbol "RECORD_NAME" package)
        (is (eq :external status))
        (is (eq (first errors) (find-class symbol)))))))

(test null-namespace
  (when (find-package "null")
    (delete-package "null"))
  (let* ((schema (make-instance 'avro:fixed :name "foo" :size 2))
         (interned (avro:intern schema :null-namespace "null")))
    (multiple-value-bind (symbol status)
        (find-symbol "foo" (find-package "null"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq schema (find-class interned)))))
