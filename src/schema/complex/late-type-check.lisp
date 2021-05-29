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
(defpackage #:cl-avro.schema.complex.late-type-check
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex.common
                #:define-initializers)
  (:import-from #:cl-avro.schema.complex.base
                #:ensure-superclass)
  (:import-from #:cl-avro.schema.complex.scalarize
                #:scalarize-class)
  (:export #:late-class
           #:parse-slot-value))
(in-package #:cl-avro.schema.complex.late-type-check)

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
                (unless (closer-mop:class-finalized-p class)
                  (closer-mop:finalize-inheritance class))))
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

(defclass late-class (scalarize-class)
  ())

(defmethod closer-mop:validate-superclass
    ((class late-class) (superclass scalarize-class))
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

(define-initializers late-class :around
    (&rest initargs)
  (ensure-superclass late-object)
  (apply #'call-next-method instance initargs))

;;; late-object

(defclass late-object ()
  ())

(defmethod closer-mop:finalize-inheritance :before
    ((instance late-object))
  ;; complex-schema would have already checked the slots against
  ;; regular/early type so we just need to perform the late-binding
  ;; checks
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
         (value (parse-slot-value
                 class
                 name
                 type
                 (when (slot-boundp class name)
                   (slot-value class name)))))
    (unless (typep value type)
      (error "Slot ~S expects type ~S: ~S" name type value))
    (setf (slot-value class name) value))
  (values))

(defgeneric parse-slot-value (class name type value)
  (:method (class (name symbol) type value)
    (declare (ignore class name))
    (if (and (symbolp value)
             (not (typep value type)))
        (find-class value)
        value)))
