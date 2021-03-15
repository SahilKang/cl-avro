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

(eval-when (:compile-toplevel)
  (defparameter +serialize-docstring+
    "Return json string representation of SCHEMA.

If CANONICAL-FORM-P is true, then the Canonical Form is returned."))

(macrolet
    ((defprimitives ()
       (flet ((make-defmethod (schema)
                `(defmethod serialize
                     ((schema (eql ',schema)) &key canonical-form-p)
                   ,+serialize-docstring+
                   (declare (ignore schema))
                   (with-output-to-string (stream)
                     (schema->json ',schema stream canonical-form-p)))))
         (let* ((primitives (mapcar #'car +primitive->name+))
                (defmethods (mapcar #'make-defmethod primitives)))
           `(progn
              ,@defmethods))))
     (defcomplex ()
       `(defmethod serialize
            ((schema complex-schema) &key canonical-form-p)
          ,+serialize-docstring+
          (with-output-to-string (stream)
            (schema->json schema stream canonical-form-p)))))
  (defprimitives)
  (defcomplex))

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
