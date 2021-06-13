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
(defpackage #:cl-avro.ipc.protocol.class.io
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:error #:cl-avro.ipc.error))
  (:import-from #:cl-avro.ipc.protocol.class.protocol
                #:protocol
                #:messages
                #:types))
(in-package #:cl-avro.ipc.protocol.class.io)

;;; serialize

(defmethod io:serialize
    ((protocol protocol) &key)
  "Return json string representation of PROTOCOL."
  (let ((schema:*seen* (make-hash-table :test #'eq)))
    (st-json:write-json-to-string (schema:to-jso protocol))))

(defmethod schema:to-jso
    ((protocol protocol))
  (let ((initargs (list "protocol" (nth-value 1 (schema:name protocol))))
        (documentation (documentation protocol t))
        (types (types protocol))
        (messages (messages protocol)))
    (multiple-value-bind (_ namespace namespacep)
        (schema:namespace protocol)
      (declare (ignore _))
      (when namespacep
        (setf initargs (nconc initargs (list "namespace" namespace)))))
    (when documentation
      (setf initargs (nconc initargs (list "doc" documentation))))
    (when types
      (let ((types (map 'vector #'schema:to-jso types)))
        (setf initargs (nconc initargs (list "types" types)))))
    (when messages
      (loop
        for message across messages
        for name = (string (closer-mop:generic-function-name message))
        for jso = (schema:to-jso message)

        collect name into messages-initargs
        collect jso into messages-initargs

        finally
           (let ((messages (apply #'st-json:jso messages-initargs)))
             (setf initargs (nconc initargs (list "messages" messages))))))
    (apply #'st-json:jso initargs)))

;;; deserialize

(defmethod io:deserialize
    ((protocol (eql 'protocol)) (input string) &key)
  "Read a protocol from the json INPUT."
  (jso->protocol (st-json:read-json input)))

(declaim
 (ftype (function (st-json:jso) (values protocol &optional)) jso->protocol))
(defun jso->protocol (jso)
  (schema:with-initargs ((protocol name) namespace (doc documentation)) jso
    (let ((schema:*fullname->schema* (schema:make-fullname->schema))
          (schema:*enclosing-namespace*
            (schema:namespace
             (make-instance
              'schema:named-class :name protocol :namespace namespace)))
          (schema:*error-on-duplicate-name-p* t))
      (declare (special schema:*fullname->schema*
                        schema:*enclosing-namespace*
                        schema:*error-on-duplicate-name-p*))
      (multiple-value-bind (types typesp)
          (st-json:getjso "types" jso)
        (when typesp
          (push (parse-types types) initargs)
          (push :types initargs)))
      (multiple-value-bind (messages messagesp)
          (st-json:getjso "messages" jso)
        (when messagesp
          (push (parse-messages messages) initargs)
          (push :messages initargs)))
      (let ((protocol (apply #'make-instance 'protocol initargs)))
        (closer-mop:finalize-inheritance protocol)
        protocol))))

(declaim
 (ftype (function (t) (values (vector cons) &optional))
        parse-messages))
(defun parse-messages (messages)
  (check-type messages st-json:jso)
  (let ((vector
          (make-array 0 :element-type 'cons :adjustable t :fill-pointer t)))
    (flet ((fill-vector (key value)
             (schema:with-initargs (one-way (doc documentation)) value
               ;; TODO I should probably just make one-way an
               ;; avro:boolean and then:
               ;;   * make avro:boolean map to t/nil
               ;;   * make null map to 'avro:null
               (when one-wayp
                 (setf (getf initargs :one-way) (eq one-way 'schema:true)))
               (push (make-symbol key) initargs)
               (push :name initargs)
               (multiple-value-bind (request requestp)
                   (st-json:getjso "request" value)
                 (when requestp
                   (push (parse-request request) initargs)
                   (push :request initargs)))
               (multiple-value-bind (response responsep)
                   (st-json:getjso "response" value)
                 (when responsep
                   (push (schema:parse-schema response) initargs)
                   (push :response initargs)))
               (multiple-value-bind (errors errorsp)
                   (st-json:getjso "errors" value)
                 (when errorsp
                   (push (parse-errors errors) initargs)
                   (push :errors initargs)))
               (vector-push-extend initargs vector))))
      (st-json:mapjso #'fill-vector messages))
    vector))

(declaim (ftype (function (t) (values list &optional)) parse-request))
(defun parse-request (request)
  (check-type request list)
  (mapcar #'%parse-request request))

(declaim (ftype (function (t) (values cons &optional)) %parse-request))
(defun %parse-request (request)
  (check-type request st-json:jso)
  (schema:with-initargs
      (name (doc documentation) order aliases default)
      request
    (when namep
      (setf (getf initargs :name) (make-symbol name)))
    (multiple-value-bind (type typep)
        (st-json:getjso "type" request)
      (when typep
        (push (schema:parse-schema type) initargs)
        (push :type initargs)))
    initargs))

(declaim (ftype (function (t) (values list &optional)) parse-errors))
(defun parse-errors (errors)
  (check-type errors list)
  (flet ((parse-error (error)
           (if (typep error 'st-json:jso)
               (%parse-error error)
               (let ((schema:*seen* (make-hash-table :test #'eq))
                     (schema:*error-on-duplicate-name-p* nil))
                 (declare (special schema:*seen*
                                   schema:*error-on-duplicate-name-p*))
                 (%parse-error
                  (schema:to-jso
                   (schema:parse-schema error)))))))
    (mapcar #'parse-error errors)))

(declaim
 (ftype (function (t) (values (simple-array (or class schema:schema) (*)) &optional))
        parse-types))
(defun parse-types (types)
  (check-type types list)
  (map '(simple-array class (*)) #'parse-type types))

(declaim
 (ftype (function ((or list simple-string st-json:jso))
                  (values (or class schema:schema) &optional))
        parse-type))
(defun parse-type (json)
  (if (and (typep json 'st-json:jso)
           (string= "error" (or (st-json:getjso "type" json) "")))
      (%parse-error json)
      (schema:parse-schema json)))

(declaim
 (ftype (function (st-json:jso) (values class &optional)) %parse-error))
(defun %parse-error (jso)
  (declare (special schema:*enclosing-namespace*))
  (schema:with-initargs (name namespace aliases (doc documentation)) jso
    (push schema:*enclosing-namespace* initargs)
    (push :enclosing-namespace initargs)
    (push (parse-fields name namespace jso) initargs)
    (push :direct-slots initargs)
    (let* ((schema (schema:register-named-schema
                    (apply #'make-instance 'schema:record initargs)))
           (fields (st-json:getjso "fields" jso))
           (schema:*enclosing-namespace* (schema:namespace schema))
           (schema:*error-on-duplicate-name-p* nil))
      (declare (special schema:*enclosing-namespace*
                        schema:*error-on-duplicate-name-p*))
      (setf (getf initargs :direct-slots) (map 'list #'parse-field fields))
      (error:to-error
       (apply #'reinitialize-instance schema initargs)))))

(declaim
 (ftype (function (schema:fullname schema:namespace st-json:jso)
                  (values list &optional))
        parse-fields))
(defun parse-fields (name namespace jso)
  (declare (special schema:*enclosing-namespace*))
  (let* ((placeholder (schema:register-named-schema
                       (make-instance
                        'schema:named-schema
                        :name name
                        :namespace namespace
                        :enclosing-namespace schema:*enclosing-namespace*)))
         (schema:*enclosing-namespace* (schema:namespace placeholder)))
    (declare (special schema:*enclosing-namespace*))
    (schema:with-fields (fields) jso
      (unless (and fieldsp (typep fields 'sequence))
        (error "Errors must provide an array of fields."))
      (prog1 (map 'list #'parse-field fields)
        (schema:unregister-named-schema placeholder)))))

(declaim (ftype (function (st-json:jso) (values list &optional)) parse-field))
(defun parse-field (jso)
  (schema:with-initargs ((doc documentation) order aliases default) jso
    (let ((name (make-symbol (st-json:getjso "name" jso)))
          (type (parse-type (st-json:getjso "type" jso))))
      (push name initargs)
      (push :name initargs)
      (push type initargs)
      (push :type initargs))
    initargs))
