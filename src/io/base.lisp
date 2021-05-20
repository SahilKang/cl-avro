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
   (#:schema #:cl-avro.schema))
  (:export #:serialize
           #:deserialize
           #:serialized-size
           #:serialize-into
           #:deserialize-from-vector
           #:deserialize-from-stream
           #:define-deserialize-from
           #:assert-match
           #:resolve))
(in-package #:cl-avro.io.base)

;;; serialize

(defgeneric serialized-size (object)
  (:documentation
   "Determine the number of bytes OBJECT will be when serialized."))

(defgeneric serialize-into (object vector start)
  (:documentation
   "Serialize OBJECT into VECTOR and return the number of serialized bytes."))

(defgeneric serialize (object &key &allow-other-keys)
  (:method (object &key into (start 0))
    "Serialize OBJECT into vector supplied by INTO, starting at START.

If INTO is nil, then a new vector will be allocated.

Return (values vector number-of-serialized-bytes)"
    (let* ((size (serialized-size object))
           (into (or into (make-array size :element-type '(unsigned-byte 8)))))
      (check-type into (simple-array (unsigned-byte 8) (*)))
      (check-type start (and (integer 0) fixnum))
      (when (> size (- (length into) start))
        (error "Not enough room in vector"))
      (serialize-into object into start)
      (values into size))))

;;; resolution

(defgeneric assert-match (reader writer)
  (:method (reader writer)
    (error "Reader schema ~S does not match writer schema ~S" reader writer))

  (:documentation
   "Asserts that schemas match for schema resolution."))

(defgeneric resolve (reader writer)
  (:method :before (reader writer)
    (assert-match reader writer))

  (:method :around ((reader symbol) writer)
    (if (typep reader 'schema:primitive-schema)
        (call-next-method)
        (resolve (find-class reader) writer)))

  (:method (reader writer)
    (declare (ignore writer))
    reader)

  (:documentation
   "Return a schema that resolves the differences between the inputs."))

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
    (deserialize-from-vector
     (if reader-schema (resolve reader-schema schema) schema)
     input
     start))

  (:method (schema (input stream) &key reader-schema)
    (deserialize-from-stream
     (if reader-schema (resolve reader-schema schema) schema)
     input))

  (:documentation
   "Deserialize an instance of SCHEMA from INPUT.

If READER-SCHEMA is provided, then deserialization is performed on the
resolved schema."))

;; macro

(eval-when (:compile-toplevel)
  (defun parse-declare (body)
    (when (consp (car body))
      (cond
        ((eq (caar body) 'declare)
         (values (car body) nil))
        ((and (consp (cadar body))
              (eq (caadar body) 'declare))
         (values (car body) t))))))

(defmacro define-deserialize-from (schema-specializer &body body)
  (multiple-value-bind (declare-form should-eval-declare-form-p)
      (parse-declare body)
    (let ((body (if declare-form (cdr body) body))
          (schema (intern "SCHEMA"))
          (vector (intern "VECTOR"))
          (start (intern "START"))
          (stream (intern "STREAM")))
      `(progn
         (defmethod deserialize-from-vector
             ((,schema ,schema-specializer) (,vector simple-array) (,start fixnum))
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

         (defmethod deserialize-from-stream
             ((,schema ,schema-specializer) (,stream stream))
           (declare ,@(cdr (if should-eval-declare-form-p
                               (eval `(let ((vectorp nil)
                                            (streamp t))
                                        (declare (ignorable vectorp streamp))
                                        ,declare-form))
                               declare-form)))
           ,(eval `(let ((vectorp nil)
                         (streamp t))
                     (declare (ignorable vectorp streamp))
                     ,@body)))))))
