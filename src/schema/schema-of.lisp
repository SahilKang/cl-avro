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
(defpackage #:cl-avro.schema.schema-of
  (:use #:cl)
  (:import-from #:cl-avro.schema.complex
                #:complex-object)
  (:import-from #:cl-avro.schema.primitive
                #:int
                #:long
                #:double
                #:bytes
                #:true
                #:false)
  (:shadowing-import-from #:cl-avro.schema.primitive
                          #:null
                          #:boolean
                          #:float
                          #:string)
  (:export #:schema-of))
(in-package #:cl-avro.schema.schema-of)

(defgeneric schema-of (object)
  (:method ((object complex-object))
    (class-of object))

  (:method ((object cl:null))
    'null)

  (:method ((object (eql 'true)))
    'boolean)

  (:method ((object (eql 'false)))
    'boolean)

  (:method ((object integer))
    (etypecase object
      (int 'int)
      (long 'long)))

  (:method ((object single-float))
    'float)

  (:method ((object double-float))
    'double)

  (:method ((object vector))
    (check-type object bytes)
    'bytes)

  (:method ((object cl:string))
    'string))
