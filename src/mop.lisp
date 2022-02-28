;;; Copyright 2021-2022 Google LLC
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
(defpackage #:cl-avro.internal.mop
  (:use #:cl)
  (:export #:definit
           #:scalar-class
           #:scalarize
           #:late-class
           #:early->late
           #:schema-class
           #:all-or-nothing-reinitialization))
(in-package #:cl-avro.internal.mop)

;;; definit

(defmacro definit
    ((instance qualifier &rest initargs) &body body)
  `(progn
     (defmethod initialize-instance ,qualifier
       (,instance ,@initargs)
       ,@body)

     (defmethod reinitialize-instance ,qualifier
       (,instance ,@initargs)
       ,@body)))

;;; ensure-superclass

(declaim
 (ftype (function (class class) (values boolean &optional)) superclassp))
(defun superclassp (superclass subclass)
  "True if SUPERCLASS is an inclusive superclass of SUBCLASS."
  (nth-value 0 (subtypep subclass superclass)))

(declaim
 (ftype (function ((or class symbol) list) (values cons &optional))
        ensure-superclass))
(defun ensure-superclass (class initargs)
  (let ((class (if (symbolp class) (find-class class) class)))
    (pushnew class (getf initargs :direct-superclasses) :test #'superclassp))
  initargs)

;;; all-or-nothing-reinitialization

(defclass all-or-nothing-reinitialization ()
  ())

(defmethod reinitialize-instance :around
    ((instance all-or-nothing-reinitialization) &rest initargs &key &allow-other-keys)
  (let ((new (apply #'make-instance (class-of instance) initargs))
        (instance (call-next-method)))
    (loop
      with terminals = (mapcar #'find-class '(standard-class standard-object))
      and classes = (list (class-of instance))

      while classes
      for class = (pop classes)
      unless (member class terminals) do
        (setf classes (append classes (closer-mop:class-direct-superclasses class)))
        (loop
          for slot in (closer-mop:class-direct-slots class)
          for name = (closer-mop:slot-definition-name slot)
          if (slot-boundp new name) do
            (setf (slot-value instance name) (slot-value new name))
          else do
            (slot-makunbound instance name)))
    (closer-mop:finalize-inheritance instance)
    instance))

;;; scalar-class

;; TODO maybe eval-when :compile-toplevel
(declaim (ftype (function (list) (values boolean &optional)) keywordsp))
(defun keywordsp (list)
  (every #'keywordp list))

(deftype list<keyword> ()
  '(and list (satisfies keywordsp)))

(defclass scalar-class (standard-class)
  ((scalars
    :initarg :scalars
    :reader scalars
    :type list<keyword>
    :documentation "Initargs to scalarize."))
  (:default-initargs
   :scalars nil))

(defmethod closer-mop:validate-superclass
    ((class scalar-class) (superclass standard-class))
  t)

(definit ((instance scalar-class) :around &rest initargs)
  (setf initargs (ensure-superclass 'scalar-object initargs))
  (apply #'call-next-method instance initargs))

;;; scalar-object

(defclass scalar-object ()
  ())

(declaim (ftype (function (cons) (values t &optional)) %scalarize))
(defun %scalarize (list)
  (if (= (length list) 1)
      (first list)
      list))

(declaim (ftype (function (t) (values t &optional)) scalarize))
(defun scalarize (value)
  (if (consp value)
      (%scalarize value)
      value))

(declaim
 (ftype (function (scalar-class) (values list &optional))
        initargs-to-scalarize))
(defun initargs-to-scalarize (class)
  (loop
    for superclass in (closer-mop:class-precedence-list class)

    when (typep superclass 'scalar-class)
      append (scalars superclass) into initargs

    finally
       (return
         (delete-duplicates initargs))))

(definit ((instance scalar-object) :around &rest initargs)
  (loop
    with initargs-to-scalarize = (initargs-to-scalarize (class-of instance))

    initially
       (assert (evenp (length initargs)))

    for (initarg value) on initargs by #'cddr

    if (member initarg initargs-to-scalarize)
      nconc (list initarg (scalarize value)) into scalarized-initargs
    else
      nconc (list initarg value) into scalarized-initargs

    finally
       (return
         (apply #'call-next-method instance scalarized-initargs))))

;;; late-slot

(defclass late-slot (closer-mop:standard-direct-slot-definition)
  ((late-type
    :type (or symbol cons)
    :reader late-type)))

(defmethod initialize-instance :around
    ((instance late-slot)
     &rest initargs
     &key
       (late-type (error "Must supply LATE-TYPE"))
       (early-type `(or symbol ,late-type)))
  (setf (getf initargs :type) early-type)
  (remf initargs :early-type)
  (remf initargs :late-type)
  (let ((instance (apply #'call-next-method instance initargs)))
    (setf (slot-value instance 'late-type) late-type)))

;;; finalizing-reader

(defclass finalizing-reader (closer-mop:standard-reader-method)
  ())

(defmethod initialize-instance :around
    ((instance finalizing-reader) &rest initargs &key slot-definition)
  (let* ((gf
           (symbol-function
            (first (closer-mop:slot-definition-readers slot-definition))))
         (lambda
             `(lambda (class)
                (closer-mop:ensure-finalized class)))
         (method-lambda
           (closer-mop:make-method-lambda
            gf (closer-mop:class-prototype (class-of instance)) lambda nil))
         (documentation
           "Finalizes class before reader method executes.")
         (primary-method
           (call-next-method))
         (before-method
           (let ((instance (allocate-instance (class-of instance))))
             (setf (getf initargs :qualifiers) '(:before)
                   (getf initargs :documentation) documentation
                   (getf initargs :function) (compile nil method-lambda))
             (apply #'call-next-method instance initargs))))
    (add-method gf before-method)
    primary-method))

;;; late-class

(defclass late-class (standard-class)
  ())

(defmethod closer-mop:validate-superclass
    ((class late-class) (superclass standard-class))
  t)

(defmethod closer-mop:direct-slot-definition-class
    ((class late-class) &rest initargs)
  (if (or (member :early-type initargs)
          (member :late-type initargs))
      (find-class 'late-slot)
      (call-next-method)))

(defmethod closer-mop:reader-method-class
    ((class late-class) slot &rest initargs)
  (declare (ignore class slot initargs))
  (find-class 'finalizing-reader))

(definit ((instance late-class) :around &rest initargs)
  (setf initargs (ensure-superclass 'late-object initargs))
  (apply #'call-next-method instance initargs))

;;; late-object

(defclass late-object ()
  ())

(defmethod closer-mop:finalize-inheritance :before
    ((instance late-object))
  (flet ((not-late-slot-p (slot)
           (not (typep slot 'late-slot)))
         (process-slot (slot)
           (process-slot instance slot)))
    (let ((slots (remove-if
                  #'not-late-slot-p
                  (closer-mop:class-direct-slots
                   (class-of instance)))))
      (map nil #'process-slot slots))))

(declaim
 (ftype (function (late-object late-slot) (values &optional)) process-slot))
(defun process-slot (class slot)
  (let* ((name (closer-mop:slot-definition-name slot))
         (type (late-type slot))
         (value (early->late
                 class
                 name
                 type
                 (when (slot-boundp class name)
                   (slot-value class name)))))
    (unless (typep value type)
      (error "Slot ~S expects type ~S: ~S" name type value))
    (setf (slot-value class name) value))
  (values))

(defgeneric early->late (class name type value)
  (:method (class (name symbol) type value)
    (declare (ignore class name))
    (if (and (symbolp value)
             (not (typep value type)))
        (find-class value)
        value)))

;;; schema-class

;; TODO rename this to object-class or something
(defclass schema-class (scalar-class late-class)
  ((object-class
    :initarg :object-class
    :reader object-class
    :type symbol))
  (:default-initargs
   :object-class (error "Must supply OBJECT-CLASS")))

(defmethod closer-mop:validate-superclass
    ((class schema-class) (superclass scalar-class))
  t)

(defmethod closer-mop:validate-superclass
    ((class schema-class) (superclass late-class))
  t)

(definit ((instance schema-class) :around &rest initargs &key object-class)
  (setf (getf initargs :object-class) (scalarize object-class)
        initargs (ensure-superclass 'schema-object initargs))
  (apply #'call-next-method instance initargs))

;;; schema-object

(defclass schema-object ()
  ())

(definit ((instance schema-object) :around &rest initargs)
  (setf initargs
        (ensure-superclass (object-class (class-of instance)) initargs))
  (apply #'call-next-method instance initargs))
