;;; Copyright 2023 Google LLC
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
(defpackage #:cl-avro/test/intern
  (:local-nicknames
   (#:avro #:cl-avro))
  (:use #:cl #:1am))
(in-package #:cl-avro/test/intern)

(test fixed
  (when (find-package "foo")
    (delete-package "foo"))
  (let* ((schema (make-instance 'avro:fixed :namespace "foo" :name "bar" :size 2))
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
  (let* ((schema (make-instance 'avro:enum :namespace "namespace" :name "enum_name"
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
  (let* ((schema (make-instance 'avro:record :namespace "namespace"
                                             :name '#:record_name
                                             :direct-slots
                                             '((:name #:foo
                                                :type avro:int
                                                :readers (#:read-foo)
                                                :writers ((setf #:write-foo))))))
         (interned (avro:intern schema)))
    (multiple-value-bind (symbol status)
        (find-symbol "RECORD_NAME" (find-package "namespace"))
      (is (eq :external status))
      (is (eq symbol interned)))
    (is (eq schema (find-class interned)))
    (let* ((package (find-package "namespace"))
           (slot (first (closer-mop:class-direct-slots schema)))
           (reader (fdefinition (first (closer-mop:slot-definition-readers slot))))
           (writer (fdefinition (first (closer-mop:slot-definition-writers slot)))))
      (multiple-value-bind (symbol status)
          (find-symbol "READ-FOO" package)
        (is (eq :external status))
        (is (eq reader (fdefinition symbol))))
      (multiple-value-bind (symbol status)
          (find-symbol "WRITE-FOO" package)
        (is (eq :external status))
        (is (eq writer (fdefinition `(setf ,symbol))))))))

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
                                     :request ((:name #:greeting :type avro:string))
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
