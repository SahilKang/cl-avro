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
(defpackage #:cl-avro.internal.ipc.error
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:cl-avro.internal.ipc.error)

(define-condition api:rpc-error (error)
  ((metadata
    :initarg :metadata
    :type internal:map<bytes> ; TODO this needs to be public if I
                              ; return map<bytes>
    :reader api:metadata
    :documentation "Metadata from server"))
  (:default-initargs
   :metadata (make-instance 'internal:map<bytes> :size 0))
  (:documentation
   "Base condition for rpc errors."))

(define-condition api:undeclared-rpc-error (api:rpc-error)
  ((message
    :initarg :message
    :type api:string
    :reader api:message
    :documentation "Error message from server."))
  (:report
   (lambda (condition stream)
     (format stream (api:message condition))))
  (:default-initargs
   :message (error "Must supply MESSAGE"))
  (:documentation
   "Undeclared error from server."))

(define-condition api:declared-rpc-error (api:rpc-error)
  ((schema
    :type api:record
    :reader internal:schema
    :allocation :class))
  (:documentation
   "Declared error from server."))

(declaim
 (ftype (function (symbol api:record-object &optional internal:map<bytes>)
                  (values api:declared-rpc-error &optional))
        internal:make-declared-rpc-error))
(defun internal:make-declared-rpc-error
    (type error &optional (metadata (make-instance 'internal:map<bytes> :size 0)))
  (loop
    for field across (api:fields (class-of error))
    for (name slot) = (multiple-value-list (api:name field))

    collect (intern name 'keyword) into initargs
    collect (slot-value error slot) into initargs

    finally
       (return
         (apply #'make-condition type (list* :metadata metadata initargs)))))

(declaim
 (ftype (function (api:declared-rpc-error)
                  (values api:record-object &optional))
        internal:to-record))
(defun internal:to-record (error)
  (loop
    with schema = (internal:schema error)

    for field across (api:fields schema)
    for (name slot) = (multiple-value-list (api:name field))

    collect (intern name 'keyword) into initargs
    collect (funcall slot error) into initargs

    finally
       (return
         (apply #'make-instance schema initargs))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (api:field) (values cons &optional)) make-condition-slot))
  (defun make-condition-slot (field)
    (let ((name (closer-mop:slot-definition-name field))
          (type (closer-mop:slot-definition-type field))
          (documentation (documentation field t))
          (initargs (closer-mop:slot-definition-initargs field))
          (readers (closer-mop:slot-definition-readers field))
          (writers (closer-mop:slot-definition-writers field)))
      `(,name
        :type ,type
        ,@(when documentation
            `(:documentation ,documentation))
        ,@(mapcan (lambda (initarg)
                    `(:initarg ,initarg))
                  initargs)
        ,@(mapcan (lambda (reader)
                    `(:reader ,reader))
                  (list* name readers))
        ,@(mapcan (lambda (writer)
                    `(:writer ,writer))
                  writers))))

  (declaim
   (ftype (function (api:record) (values list &optional)) make-condition-slots))
  (defun make-condition-slots (record)
    (mapcar #'make-condition-slot (closer-mop:class-direct-slots record)))

  (declaim
   (ftype (function (symbol list list api:record) (values cons &optional))
          expand-define-condition))
  (defun expand-define-condition (name slots options record)
    `(progn
       (define-condition ,name (api:declared-rpc-error)
         ((schema :allocation :class :initform ,record)
          ,@slots)
         ,@options)

       (find-class ',name))))

(declaim (ftype (function (api:record) (values class &optional)) to-error))
(defun to-error (record)
  (let ((condition-slots (make-condition-slots record))
        (condition-options (when (documentation record t)
                             `((:documentation ,(documentation record t)))))
        (name (make-symbol (api:name record))))
    (eval
     (expand-define-condition name condition-slots condition-options record))))

(defmethod internal:write-jso
    ((error class) seen canonical-form-p)
  (assert (subtypep error 'api:declared-rpc-error) ())
  (closer-mop:ensure-finalized error)
  (let* ((record (internal:schema (closer-mop:class-prototype error)))
         (jso (internal:write-jso record seen canonical-form-p)))
    (when (typep jso 'st-json:jso)
      (setf (st-json:getjso "type" jso) "error"))
    jso))

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "error")) fullname->schema enclosing-namespace)
      (setf (st-json:getjso "type" jso) "record")
      (let* ((record (internal:read-jso jso fullname->schema enclosing-namespace))
             (condition (to-error record)))
        (setf (gethash (api:fullname record) fullname->schema) condition)
        condition)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (ftype (function (list) (values &optional)) assert-valid-options))
  (defun assert-valid-options (options)
    (loop
      ;; TODO should I add :name that overrides like named-schemas?
      with expected-keys = '(:namespace :enclosing-namespace :aliases
                             :default-initargs :documentation :report)
        initially
           (assert (every #'consp options) (options)
                   "Expected a list of cons cells: ~S" options)

      for (key &rest) in options
      unless (member key expected-keys) do
        (error "Unknown key ~S, expected one of ~S" key expected-keys)
      collect key into keys
      finally
         (loop
           for expected-key in expected-keys
           do (setf keys (delete expected-key keys :count 1))
           finally
              (when keys
                (error "Duplicate keys: ~S" keys))))
    (values))

  (declaim
   (ftype (function (list) (values list &optional)) options->record-initargs))
  (defun options->record-initargs (options)
    (loop
      with keys = '(:namespace :enclosing-namespace :aliases :documentation)

      for key in keys
      for assoc = (assoc key options)
      when assoc
        nconc (list key (cdr assoc))))

  (declaim
   (ftype (function (list) (values list &optional)) options->condition-options))
  (defun options->condition-options (options)
    (loop
      with keys = '(:default-initargs :documentation :report)

      for key in keys
      when (assoc key options)
        collect it))

  (declaim
   (ftype (function (list symbol symbol (function (t) (values t &optional)))
                    (values list &optional))
          %gather-into))
  (defun %gather-into (field from to transform)
    (loop
      while (member from field)
      collect (funcall transform (getf field from)) into gathered
      do (remf field from)

      finally
         (when gathered
           (setf (getf field to) gathered))
         (return field)))

  (declaim
   (ftype (function
           (list symbol symbol &optional (function (t) (values t &optional)))
           (values list &optional))
          gather-into))
  (defun gather-into (fields from to &optional (transform #'identity))
    (flet ((%gather-into (field)
             (%gather-into field from to transform)))
      (mapcar #'%gather-into fields)))

  (declaim (ftype (function (list) (values list &optional)) to-direct-slots))
  (defun to-direct-slots (fields)
    (flet ((add-name (field)
             (push :name field))
           (expand-accessors (field)
             (loop
               while (member :accessor field) do
                 (push (getf field :accessor) field)
                 (push :reader field)
                 (push (getf field :accessor) field)
                 (push :writer field)
                 (remf field :accessor)

               finally
                  (return field)))
           (writer-transform (writer)
             (declare ((or cons symbol) writer))
             (if (consp writer)
                 writer
                 `(setf ,writer))))
      (setf fields (mapcar #'add-name fields)
            fields (mapcar #'expand-accessors fields)
            fields (gather-into fields :reader :readers)
            fields (gather-into fields :writer :writers #'writer-transform)
            fields (gather-into fields :initarg :initargs)))))

(defmacro api:define-error (name (&rest fields) &body options)
  (declare (symbol name))
  (assert-valid-options options)
  (let ((record-initargs (list*
                          ;; if this is the same symbol as name, then
                          ;; I can't use the ctor twice
                          :name (make-symbol (string name))
                          :direct-slots (to-direct-slots fields)
                          (options->record-initargs options)))
        (condition-options (options->condition-options options))
        (record (gensym))
        (condition-slots (gensym)))
    `(eval-when (:compile-toplevel :load-toplevel :execute)
       (let* ((,record (apply #'make-instance 'api:record ',record-initargs))
              (,condition-slots (make-condition-slots ,record)))
         (eval
          (expand-define-condition ',name ,condition-slots ',condition-options ,record))))))
