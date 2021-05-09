;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.primitive
  (:use #:cl)
  (:shadow #:null
           #:boolean
           #:float
           #:string)
  (:export #:primitive-schema
           #:primitive-object
           #:+primitive->name+

           #:null
           #:boolean #:true #:false
           #:int
           #:long
           #:float
           #:double
           #:bytes
           #:string))
(in-package #:cl-avro.schema.primitive)

(eval-when (:compile-toplevel)
  (defparameter *primitives* nil
    "List of primitive schemas."))

(defmacro defprimitive (name base &body docstring)
  "Define a primitive schema NAME based on BASE."
  (declare (symbol name)
           ((or symbol cons) base))
  (pushnew name *primitives* :test #'eq)
  `(deftype ,name ()
     ,@docstring
     ',base))


(defprimitive null cl:null
  "Represents the avro null schema.")

(defprimitive boolean (member true false)
  "Represents the avro boolean schema.")

(defprimitive int (signed-byte 32)
  "Represents the avro int schema.")

(defprimitive long (signed-byte 64)
  "Represents the avro long schema.")

(defprimitive float
    #.(let ((float-digits (float-digits 1f0)))
        (unless (= 24 float-digits)
          (error "(float-digits 1f0) is ~S and not 24" float-digits))
        'single-float)
  "Represents the avro float schema.")

(defprimitive double
    #.(let ((float-digits (float-digits 1d0)))
        (unless (= 53 float-digits)
          (error "(float-digits 1d0) is ~S and not 53" float-digits))
        'double-float)
  "Represents the avro double schema.")

(defprimitive bytes
    #.(let ((upgraded-type (upgraded-array-element-type '(unsigned-byte 8))))
        (unless (equal upgraded-type '(unsigned-byte 8))
          (error "byte array gets upgraded to ~S" upgraded-type))
        '(vector (unsigned-byte 8)))
  "Represents the avro bytes schema.")

(defprimitive string cl:string
  "Represents the avro string schema.")


(macrolet
    ((deftypes ()
       `(progn
          (deftype primitive-schema ()
            "The set of avro primitive schemas."
            '(member ,@*primitives*))

          (deftype primitive-object ()
            "The set of objects adhering to an avro primitive schema."
            '(or ,@*primitives*)))))
  (deftypes))

(eval-when (:compile-toplevel)
  (defparameter +primitive->name+
    (labels
        ((name (schema)
           (string-downcase (symbol-name schema)))
         (make-pair (schema)
           (cons schema (name schema))))
      (mapcar #'make-pair *primitives*))
    "An alist mapping primitive schemas to their names."))
