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
(defpackage #:cl-avro.schema.complex.fixed
  (:use #:cl)
  (:local-nicknames
   (#:sequences #:org.shirakumo.trivial-extensible-sequences))
  (:import-from #:cl-avro.schema.complex.common
                #:raw-buffer)
  (:import-from #:cl-avro.schema.complex.base
                #:complex-schema
                #:ensure-superclass)
  (:import-from #:cl-avro.schema.complex.named
                #:named-schema
                #:name
                #:namespace
                #:fullname
                #:aliases)
  (:export #:fixed
           #:fixed-object
           #:raw-buffer
           #:size
           #:name
           #:namespace
           #:fullname
           #:aliases))
(in-package #:cl-avro.schema.complex.fixed)

(defclass fixed-object (#+sbcl sequence #-sbcl sequences:sequence)
  ((buffer
    :accessor buffer
    :reader raw-buffer
    :type (simple-array (unsigned-byte 8) (*))))
  (:metaclass complex-schema)
  (:documentation
   "Base class for objects adhering to an avro fixed schema."))

(defclass fixed (named-schema)
  ((size
    :initarg :size
    :reader size
    :type (integer 0)
    :documentation "Fixed schema size."))
  (:default-initargs
   :size (error "Must supply SIZE"))
  (:documentation
   "Base class for avro fixed schemas."))

(defmethod closer-mop:validate-superclass
    ((class fixed) (superclass named-schema))
  t)

(defmethod initialize-instance :after
    ((instance fixed-object)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let ((size (size (class-of instance)))
        (keyword-args (list :element-type '(unsigned-byte 8))))
    (when initial-element-p
      (cl:push initial-element keyword-args)
      (cl:push :initial-element keyword-args))
    (when initial-contents-p
      (cl:push initial-contents keyword-args)
      (cl:push :initial-contents keyword-args))
    (setf (buffer instance) (apply #'make-array size keyword-args))))

(declaim
 (ftype (function ((integer 0)) (values cons &optional)) make-buffer-slot))
(defun make-buffer-slot (size)
  (list :name 'buffer
        :type `(simple-array (unsigned-byte 8) (,size))))

(defmethod initialize-instance :around
    ((instance fixed) &rest initargs &key size)
  (let ((buffer-slot (make-buffer-slot size)))
    (push buffer-slot (getf initargs :direct-slots)))
  (ensure-superclass fixed-object)
  (apply #'call-next-method instance initargs))

(defmethod sequences:length
    ((instance fixed-object))
  (length (buffer instance)))

(defmethod sequences:elt
    ((instance fixed-object) (index integer))
  (elt (buffer instance) index))

(defmethod (setf sequences:elt)
    (value (instance fixed-object) (index integer))
  (setf (elt (buffer instance) index) value))

(defmethod sequences:adjust-sequence
    ((instance fixed-object)
     (length integer)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let ((size (size (class-of instance)))
        (keyword-args (list :element-type '(unsigned-byte 8))))
    (unless (= length size)
      (error "Cannot change size of fixed object from ~S to ~S" size length))
    (when initial-element-p
      (cl:push initial-element keyword-args)
      (cl:push :initial-element keyword-args))
    (when initial-contents-p
      (cl:push initial-contents keyword-args)
      (cl:push :initial-contents keyword-args))
    (setf (buffer instance)
          (apply #'adjust-array (buffer instance) length keyword-args)))
  instance)

(defmethod sequences:make-sequence-like
    ((instance fixed-object)
     (length integer)
     &rest keyword-args
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (declare (ignore initial-element initial-contents))
  (if (or initial-element-p initial-contents-p)
      (apply #'make-instance (class-of instance) keyword-args)
      (if (slot-boundp instance 'buffer)
          (make-instance (class-of instance) :initial-contents (buffer instance))
          (make-instance (class-of instance)))))

(deftype index ()
  '(integer 0))

(defmethod sequences:make-sequence-iterator
    ((instance fixed-object) &key (start 0) (end (length instance)) from-end)
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
       (declare (fixed-object sequence)
                (index iterator))
       (elt sequence iterator))
     (lambda (value sequence iterator)
       (declare (fixed-object sequence)
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
