;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.io.schema
  (:use #:cl)
  (:import-from #:cl-avro.io.base
                #:serialize
                #:deserialize)
  (:import-from #:cl-avro.schema
                #:+primitive->name+
                #:schema
                #:complex-schema
                #:schema->json
                #:json->schema)
  (:export #:serialize
           #:deserialize))
(in-package #:cl-avro.io.schema)

;;; serialize

(defmacro call-next-method-with-string-stream-if-needed
    (schema initargs stream)
  (declare (symbol initargs stream))
  `(if ,stream
       (call-next-method)
       (with-output-to-string (,stream)
         (setf (getf ,initargs :stream) ,stream)
         (apply #'call-next-method ,schema ,initargs))))

(eval-when (:compile-toplevel)
  (defparameter +serialize-docstring+
    "Write json representation of SCHEMA into STREAM.

If CANONICAL-FORM-P is true, then the Canonical Form is written.
If STREAM is nil, then the json string is returned, instead."))

;; primitive

(macrolet
    ((defprimaries ()
       (flet ((make-defmethod (schema)
                `(defmethod serialize
                     ((schema (eql ',schema)) &key stream canonical-form-p)
                   ,+serialize-docstring+
                   (declare (ignore schema))
                   (schema->json ',schema stream canonical-form-p))))
         (let* ((primitives (mapcar #'car +primitive->name+))
                (defmethods (mapcar #'make-defmethod primitives)))
           `(progn
              ,@defmethods))))
     (defarounds ()
       (flet ((make-defmethod (schema)
                `(defmethod serialize :around
                     ((schema (eql ',schema)) &rest initargs &key stream)
                   (declare (ignore schema))
                   (call-next-method-with-string-stream-if-needed
                    ',schema initargs stream))))
         (let* ((primitives (mapcar #'car +primitive->name+))
                (defmethods (mapcar #'make-defmethod primitives)))
           `(progn
              ,@defmethods)))))
  (defprimaries)
  (defarounds))

;; complex

(defmethod serialize :around
    ((schema complex-schema) &rest initargs &key stream)
  (call-next-method-with-string-stream-if-needed
   schema initargs stream))

(macrolet
    ((make-defmethod ()
       `(defmethod serialize
            ((schema complex-schema) &key stream canonical-form-p)
          ,+serialize-docstring+
          (schema->json schema stream canonical-form-p))))
  (make-defmethod))

;;; deserialize

(macrolet
    ((make-defmethod (input-specializer)
       (declare ((member string stream) input-specializer))
       `(defmethod deserialize
            ((schema (eql 'schema)) (input ,input-specializer) &key)
          "Read an avro schema from the json INPUT."
          (json->schema input))))
  (make-defmethod string)
  (make-defmethod stream))
