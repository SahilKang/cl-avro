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
(defpackage #:cl-avro.ipc.error
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.schema
                #:schema)
  (:import-from #:cl-avro.ipc.handshake
                #:map<bytes>)
  (:export #:rpc-error
           #:metadata
           #:undeclared-rpc-error
           #:message
           #:declared-rpc-error
           #:declared-rpc-error-class-p
           #:schema
           #:make-declared-rpc-error
           #:to-record
           #:to-error
           #:define-error))
(in-package #:cl-avro.ipc.error)

(define-condition rpc-error (error)
  ((metadata
    :initarg :metadata
    :reader metadata
    :type map<bytes>
    :documentation "Metadata from server."))
  (:default-initargs
   :metadata (make-instance 'map<bytes> :size 0))
  (:documentation
   "Base condition for rpc errors."))

(define-condition undeclared-rpc-error (rpc-error)
  ((message
    :initarg :message
    :reader message
    :type schema:string
    :documentation "Error message from server."))
  (:report
   (lambda (condition stream)
     (format stream (message condition))))
  (:default-initargs
   :message (error "Must supply MESSAGE"))
  (:documentation
   "Undeclared error from server."))

(define-condition declared-rpc-error (rpc-error)
  ()
  (:documentation
   "Declared error from server."))

(defgeneric declared-rpc-error-class-p (class)
  (:method (class)
    nil)
  (:documentation
   "Determines if CLASS is a declared-rpc-error class."))

(declaim
 (ftype (function (symbol schema:record-object &optional map<bytes>)
                  (values declared-rpc-error &optional))
        make-declared-rpc-error))
(defun make-declared-rpc-error
    (type error &optional (metadata (make-instance 'map<bytes> :size 0)))
  (loop
    for field across (schema:fields (class-of error))
    for (name slot) = (multiple-value-list (schema:name field))

    collect (intern name 'keyword) into initargs
    collect (slot-value error slot) into initargs

    finally
       (return
         (apply #'make-condition type (list* :metadata metadata initargs)))))

(declaim
 (ftype (function (declared-rpc-error) (values schema:record-object &optional))
        to-record))
(defun to-record (error)
  (loop
    with schema = (schema (class-of error))

    for field across (schema:fields schema)
    for (name slot) = (multiple-value-list (schema:name field))

    collect (intern name 'keyword) into initargs
    collect (funcall slot error) into initargs

    finally
       (return
         (apply #'make-instance schema initargs))))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (schema:field) (values cons &optional)) make-condition-slot))
  (defun make-condition-slot (field)
    (let ((name (nth-value 1 (schema:name field)))
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
   (ftype (function (schema:record) (values list &optional))
          make-condition-slots))
  (defun make-condition-slots (record)
    (mapcar #'make-condition-slot (closer-mop:class-direct-slots record))))

(defmacro to-error (record-form)
  (let ((record (gensym))
        (condition-slots (gensym))
        (condition-options (gensym))
        (name (gensym)))
    `(let* ((,record ,record-form)
            (,condition-slots (make-condition-slots ,record))
            (,condition-options (when (documentation ,record t)
                                  `((:documentation ,(documentation ,record t)))))
            (,name (make-symbol (schema:name ,record))))
       (eval
        `(progn
           (define-condition ,,name (declared-rpc-error)
             ,,condition-slots
             ,@,condition-options)
           (defmethod schema ((class (eql (find-class ',,name))))
             ,,record)
           (defmethod declared-rpc-error-class-p ((class (eql (find-class ',,name))))
             t)
           (defmethod schema:to-jso ((class (eql (find-class ',,name))))
             (declare (ignore class))
             (let ((jso (schema:to-jso ,,record)))
               (unless (stringp jso)
                 (setf (st-json:getjso "type" jso) "error"))
               jso))
           (find-class ',,name))))))

(eval-when (:compile-toplevel)
  (declaim (ftype (function (list) (values &optional)) assert-valid-options))
  (defun assert-valid-options (options)
    (loop
      ;; TODO should I add :name that overrides like named-schemas?
      with expected-keys = '(:namespace :enclosing-namespace :aliases
                             :default-initargs :documentation :report)
        initially
           (unless (every #'consp options)
             (error "Expected a list of cons cells: ~S" options))

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

  (declaim (ftype (function (list) (values list &optional))
                  options->record-initargs))
  (defun options->record-initargs (options)
    (loop
      with keys = '(:namespace :enclosing-namespace :aliases :documentation)

      for key in keys
      for assoc = (assoc key options)
      when assoc
        nconc (list key (cdr assoc))))

  (declaim (ftype (function (list) (values list &optional))
                  options->condition-options))
  (defun options->condition-options (options)
    (loop
      with keys = '(:default-initargs :documentation :report)

      for key in keys
      when (assoc key options)
        collect it))

  (declaim
   (ftype (function (list
                     symbol
                     symbol
                     (function (t) (values t &optional)))
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
   (ftype (function (list
                     symbol
                     symbol
                     &optional (function (t) (values t &optional)))
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

(defmacro define-error (name (&rest fields) &body options)
  (declare (symbol name))
  (assert-valid-options options)
  (let* ((record-initargs (list*
                           ;; if this is the same symbol as name, then
                           ;; I can't use the ctor twice
                           :name (make-symbol (string name))
                           :direct-slots (to-direct-slots fields)
                           (options->record-initargs options)))
         (condition-options (options->condition-options options))
         (condition-slots (make-condition-slots
                           (apply #'make-instance 'schema:record record-initargs)))
         (record (gensym)))
    `(progn
       (define-condition ,name (declared-rpc-error)
         ,condition-slots
         ,@condition-options)
       (defmethod declared-rpc-error-class-p ((class (eql (find-class ',name))))
         t)
       (let ((,record (apply #'make-instance 'schema:record ',record-initargs)))
         (defmethod schema ((class (eql (find-class ',name))))
           ,record)
         (defmethod schema:to-jso ((class (eql (find-class ',name))))
           (declare (ignore class))
           (let ((jso (schema:to-jso ,record)))
             (unless (stringp jso)
               (setf (st-json:getjso "type" jso) "error"))
             jso))
         (find-class ',name)))))
