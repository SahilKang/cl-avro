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
(defpackage #:cl-avro.io.base
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:resolution #:cl-avro.io.resolution))
  (:export #:serialize
           #:deserialize
           #:serialized-size
           #:serialize-into-vector
           #:serialize-into-stream
           #:define-serialize-into
           #:deserialize-from-vector
           #:deserialize-from-stream
           #:define-deserialize-from))
(in-package #:cl-avro.io.base)

;;; serialize

(defgeneric serialized-size (object)
  (:documentation
   "Determine the number of bytes OBJECT will be when serialized."))

(defgeneric serialize-into-vector (object vector start)
  (:documentation
   "Serialize OBJECT into VECTOR and return the number of serialized bytes."))

(defgeneric serialize-into-stream (object stream)
  (:documentation
   "Serialize OBJECT into STREAM and return the number of serialized bytes."))

(defgeneric serialize (object &key &allow-other-keys)
  (:method (object &key into (start 0))
    "Serialize OBJECT into stream or vector supplied by INTO.

If INTO is a vector, then START specifies the vector offset to start
writing into.

If INTO is nil, then a new vector will be allocated and returned.

Return (values INTO number-of-serialized-bytes)"
    (let ((size (serialized-size object)))
      (if (streamp into)
          (serialize-into-stream object into)
          (progn
            (if into
                (check-type into (simple-array (unsigned-byte 8) (*)))
                (setf into (make-array size :element-type '(unsigned-byte 8))))
            (check-type start (and (integer 0) fixnum))
            (when (> size (- (length into) start))
              (error "Not enough room in vector"))
            (serialize-into-vector object into start)))
      (values into size))))

;;; deserialize

(defgeneric deserialize-from-vector (schema vector start)
  (:documentation
   "Deserialize an instance of SCHEMA from VECTOR, starting at START.

Return (values deserialized-object number-of-bytes-deserialized)"))

(defgeneric deserialize-from-stream (schema stream)
  (:documentation
   "Deserialize an instance of SCHEMA from STREAM.

Return (values deserialized-object number-of-bytes-deserialized)"))

(defgeneric deserialize (schema input &key &allow-other-keys)
  (:method ((schema symbol) input &rest keyword-args)
    (if (typep schema 'schema:primitive-schema)
        (call-next-method)
        (apply #'deserialize (find-class schema) input keyword-args)))

  (:method (schema (input simple-array) &key reader-schema (start 0))
    (check-type input (simple-array (unsigned-byte 8) (*)))
    (check-type start (and (integer 0) fixnum))
    (multiple-value-bind (deserialized bytes-read)
        (deserialize-from-vector schema input start)
      (values
       (resolution:coerce deserialized (or reader-schema schema))
       bytes-read)))

  (:method (schema (input stream) &key reader-schema)
    (multiple-value-bind (deserialized bytes-read)
        (deserialize-from-stream schema input)
      (values
       (resolution:coerce deserialized (or reader-schema schema))
       bytes-read)))

  (:documentation
   "Deserialize an instance of SCHEMA from INPUT.

If READER-SCHEMA is provided, then schema resolution is performed to
return an instance of READER-SCHEMA, instead."))

;;; macros

(eval-when (:compile-toplevel)
  (defun parse-declare (body)
    (when (consp (car body))
      (cond
        ((eq (caar body) 'declare)
         (values (car body) nil))
        ((and (consp (cadar body))
              (eq (caadar body) 'declare))
         (values (car body) t)))))

  (defun parse-documentation (body)
    (cond
      ((stringp (first body))
       (values (first body) (rest body)))
      ((stringp (second body))
       (values (second body) (cons (first body) (cddr body))))
      (t
       (values nil body))))

  (defun parse-body (body)
    (multiple-value-bind (documentation body)
        (parse-documentation body)
      (multiple-value-bind (declare-form should-eval-declare-form-p)
          (parse-declare body)
        (values
         (if declare-form (rest body) body)
         documentation
         declare-form
         should-eval-declare-form-p))))

  (defun %find-method (prefix suffix)
    (declare (symbol prefix suffix))
    (let ((name (format nil "~A-~A" prefix suffix)))
      (multiple-value-bind (method existsp)
          (intern name)
        (unless existsp
          (error "Symbol ~S does not exist" name))
        (unless (fboundp method)
          (error "~S does not name a function" name))
        method)))

  (defun vector/stream-methods (method-prefix arg specializer body)
    (declare (symbol method-prefix arg)
             (list body)
             ((or symbol cons) specializer))
    (multiple-value-bind
          (body documentation declare-form should-eval-declare-form-p)
        (parse-body body)
      (let ((vector-method (%find-method method-prefix 'vector))
            (stream-method (%find-method method-prefix 'stream))
            (arg (intern (string arg)))
            (vector (intern "VECTOR"))
            (start (intern "START"))
            (stream (intern "STREAM")))
        `(progn
           (defmethod ,vector-method
               ((,arg ,specializer) (,vector simple-array) (,start fixnum))
             ,@(when documentation (list documentation))
             (declare ((simple-array (unsigned-byte 8) (*)) ,vector)
                      ,@(cdr (if should-eval-declare-form-p
                                 (eval `(let ((vectorp t)
                                              (streamp nil))
                                          (declare (ignorable vectorp streamp))
                                          ,declare-form))
                                 declare-form)))
             ,(eval `(let ((vectorp t)
                           (streamp nil))
                       (declare (ignorable vectorp streamp))
                       ,@body)))

           (defmethod ,stream-method
               ((,arg ,specializer) (,stream stream))
             ,@(when documentation (list documentation))
             (declare ,@(cdr (if should-eval-declare-form-p
                                 (eval `(let ((vectorp nil)
                                              (streamp t))
                                          (declare (ignorable vectorp streamp))
                                          ,declare-form))
                                 declare-form)))
             ,(eval `(let ((vectorp nil)
                           (streamp t))
                       (declare (ignorable vectorp streamp))
                       ,@body))))))))

(defmacro define-deserialize-from (schema-specializer &body body)
  (declare ((or symbol cons) schema-specializer))
  (vector/stream-methods
      '#:deserialize-from '#:schema schema-specializer body))

(defmacro define-serialize-into (object-specializer &body body)
  (declare ((or symbol cons) object-specializer))
  (vector/stream-methods
      '#:serialize-into '#:object object-specializer body))
