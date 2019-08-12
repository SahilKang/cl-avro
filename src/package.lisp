;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defpackage #:cl-avro
  (:nicknames #:avro)
  (:use #:cl)
  (:export

   ;; primitive avro schemas
   #:null-schema
   #:boolean-schema
   #:int-schema
   #:long-schema
   #:float-schema
   #:double-schema
   #:bytes-schema
   #:string-schema

   ;; complex avro schemas
   #:fixed-schema
   #:union-schema
   #:array-schema
   #:map-schema
   #:enum-schema
   #:record-schema))
