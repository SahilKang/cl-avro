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
(defpackage #:cl-avro.internal.defprimitive
  (:use #:cl)
  (:local-nicknames
   (#:little-endian #:cl-avro.internal.little-endian))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:uint64
                #:array<uint8>)
  (:import-from #:cl-avro.internal.crc-64-avro
                #:crc-64-avro)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:import-from #:cl-avro.internal
                #:read-jso
                #:write-jso
                #:crc-64-avro-little-endian)
  (:import-from #:alexandria
                #:define-constant)
  (:export #:defprimitive
           #:*primitives*))
(in-package #:cl-avro.internal.defprimitive)

(eval-when (:compile-toplevel)
  (defparameter *primitives* nil
    "List of primitive schemas."))

(defmacro defprimitive (name base &body docstring)
  "Define a primitive schema NAME based on BASE."
  (declare (symbol name)
           ((or symbol cons) base))
  (pushnew name *primitives* :test #'eq)
  (let ((+jso+ (intern "+JSO+"))
        (+json+ (intern "+JSON+"))
        (+crc-64-avro+ (intern "+CRC-64-AVRO+"))
        (+crc-64-avro-little-endian+ (intern "+CRC-64-AVRO-LITTLE-ENDIAN+"))
        (string (string-downcase (string name))))
    `(progn
       (deftype ,name ()
         ,@docstring
         ',base)

       (declaim ((eql ,name) ,name))
       (defconstant ,name ',name
         ,@docstring)

       (declaim (simple-string ,+jso+))
       (define-constant ,+jso+ ,string :test #'string=)

       (declaim (simple-string ,+json+))
       (define-constant ,+json+
           (st-json:write-json-to-string ,+jso+)
         :test #'string=)

       (declaim (uint64 ,+crc-64-avro+))
       (defconstant ,+crc-64-avro+
         (crc-64-avro (babel:string-to-octets ,+json+ :encoding :utf-8)))

       (declaim ((array<uint8> 8) ,+crc-64-avro-little-endian+))
       (define-constant ,+crc-64-avro-little-endian+
           (let ((vector (make-array 8 :element-type 'uint8)))
             (little-endian:uint64->vector ,+crc-64-avro+ vector 0)
             vector)
         :test #'equalp)

       (defmethod crc-64-avro-little-endian
           ((schema (eql ',name)))
         (declare (ignore schema))
         ,+crc-64-avro-little-endian+)

       (define-pattern-method 'read-jso
           '(lambda ((jso ("type" ,string)) fullname->schema enclosing-namespace)
             (declare (ignore jso fullname->schema enclosing-namespace))
             ,name))

       (define-pattern-method 'read-jso
           '(lambda ((jso ,string) fullname->schema enclosing-namespace)
             (declare (ignore jso fullname->schema enclosing-namespace))
             ,name))

       (defmethod write-jso
           ((schema (eql ',name)) seen canonical-form-p)
         (declare (ignore schema seen canonical-form-p))
         ,+jso+))))
