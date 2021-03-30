;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.complex.array
  (:use #:cl)
  (:shadow #:array #:push #:pop)
  (:local-nicknames
   (#:sequences #:org.shirakumo.trivial-extensible-sequences))
  (:import-from #:cl-avro.schema.complex.common
                #:raw-buffer)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass
                #:schema)
  (:export #:array
           #:array-object
           #:raw-buffer
           #:items
           #:push
           #:pop))
(in-package #:cl-avro.schema.complex.array)

(defclass array-object (#+sbcl sequence #-sbcl sequences:sequence)
  ((buffer
    :accessor buffer
    :reader raw-buffer
    :type (vector schema)))
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro array schema."))

(defclass array (complex-schema)
  ((items
    :initarg :items
    :reader items
    :type schema
    :documentation "Array schema element type."))
  (:default-initargs
   :items (error "Must supply ITEMS"))
  (:documentation
   "Base class for avro array schemas."))

(defmethod closer-mop:validate-superclass
    ((class array) (superclass complex-schema))
  t)

(declaim
 (ftype (function (schema t) (values &optional)) assert-type)
 (inline assert-type))
(defun assert-type (schema object)
  (unless (typep object schema)
    (error "Expected type ~S, but got ~S for ~S"
           schema (type-of object) object))
  (values))
(declaim (notinline assert-type))

(defmethod initialize-instance :after
    ((instance array-object)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p)
       (length (length initial-contents)))
  (declare (inline assert-type))
  (let* ((schema (items (class-of instance)))
         (keyword-args (list :element-type schema :fill-pointer t :adjustable t)))
    (when initial-element-p
      (assert-type schema initial-element)
      (cl:push initial-element keyword-args)
      (cl:push :initial-element keyword-args))
    (when initial-contents-p
      (flet ((assert-type (object)
               (assert-type schema object)))
        (map nil #'assert-type initial-contents))
      (cl:push initial-contents keyword-args)
      (cl:push :initial-contents keyword-args))
    (setf (buffer instance) (apply #'make-array length keyword-args))))

(declaim (ftype (function (schema) (values cons &optional)) make-buffer-slot))
(defun make-buffer-slot (items)
  (list :name 'buffer
        :type `(vector ,items)))

(defmethod initialize-instance :around
    ((instance array) &rest initargs &key items)
  (let ((buffer-slot (make-buffer-slot items)))
    (cl:push buffer-slot (getf initargs :direct-slots)))
  (ensure-superclass array-object)
  (apply #'call-next-method instance initargs))

(defmethod sequences:length
    ((instance array-object))
  (length (buffer instance)))

(defmethod sequences:elt
    ((instance array-object) (index integer))
  (elt (buffer instance) index))

(defmethod (setf sequences:elt)
    (value (instance array-object) (index integer))
  (let ((schema (items (class-of instance))))
    (assert-type schema value))
  (setf (elt (buffer instance) index) value))

(defmethod sequences:adjust-sequence
    ((instance array-object)
     (length integer)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let* ((schema (items (class-of instance)))
         (keyword-args (list :element-type schema :fill-pointer t)))
    (when initial-element-p
      (assert-type schema initial-element)
      (cl:push initial-element keyword-args)
      (cl:push :initial-element keyword-args))
    (when initial-contents-p
      (flet ((assert-type (object)
               (assert-type schema object)))
        (map nil #'assert-type initial-contents))
      (cl:push initial-contents keyword-args)
      (cl:push :initial-contents keyword-args))
    (setf (buffer instance)
          (apply #'adjust-array (buffer instance) length keyword-args)))
  instance)

(defmethod sequences:make-sequence-like
    ((instance array-object)
     (length integer)
     &rest keyword-args
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (declare (ignore initial-element initial-contents))
  (if (or initial-element-p initial-contents-p)
      (apply #'make-instance (class-of instance)
             (list* :length length keyword-args))
      (if (slot-boundp instance 'buffer)
          (make-instance (class-of instance)
                         :length length :initial-contents (buffer instance))
          (make-instance (class-of instance) :length length))))

(deftype index ()
  '(integer 0))

(defmethod sequences:make-sequence-iterator
    ((instance array-object) &key (start 0) (end (length instance)) from-end)
  (let* ((end (or end (length instance)))
         (iterator (if from-end (1- end) start))
         (limit (if from-end (1- start) end)))
    (values
     iterator
     limit
     from-end
     (if from-end
         (lambda (sequence iterator from-end)
           (declare (ignore sequence from-end)
                    (index iterator))
           (the index (1- iterator)))
         (lambda (sequence iterator from-end)
           (declare (ignore sequence from-end)
                    (index iterator))
           (the index (1+ iterator))))
     (lambda (sequence iterator limit from-end)
       (declare (ignore sequence from-end)
                (index iterator)
                ((or index null) limit))
       (= iterator limit))
     (lambda (sequence iterator)
       (declare (array-object sequence)
                (index iterator))
       (elt sequence iterator))
     (lambda (value sequence iterator)
       (declare (array-object sequence)
                (index iterator))
       (setf (elt sequence iterator) value))
     (lambda (sequence iterator)
       (declare (ignore sequence)
                (index iterator))
       iterator)
     (lambda (sequence iterator)
       (declare (ignore sequence)
                (index iterator))
       iterator))))

(defgeneric push (element array)
  (:method (element (instance array-object))
    (declare (inline assert-type))
    (let ((schema (items (class-of instance))))
      (assert-type schema element))
    (vector-push-extend element (buffer instance))))

(defgeneric pop (array)
  (:method ((instance array-object))
    (vector-pop (buffer instance))))
