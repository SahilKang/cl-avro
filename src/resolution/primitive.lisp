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
(defpackage #:cl-avro.resolution.primitive
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:base #:cl-avro.resolution.base)))
(in-package #:cl-avro.resolution.primitive)

;;; promote int to long

(defmethod base:coerce
    ((object integer) (schema (eql 'schema:long)))
  (declare (ignore schema))
  (check-type object schema:int)
  object)

;;; promote int and long to float

(defmethod base:coerce
    ((object integer) (schema (eql 'schema:float)))
  (declare (ignore schema))
  (check-type object (or schema:int schema:long))
  (coerce object 'schema:float))

;;; promote int and long to double

(defmethod base:coerce
    ((object integer) (schema (eql 'schema:double)))
  (declare (ignore schema))
  (check-type object (or schema:int schema:long))
  (coerce object 'schema:double))

;;; promote float to double

(defmethod base:coerce
    ((object single-float) (schema (eql 'schema:double)))
  (declare (ignore schema))
  (coerce object 'schema:double))

;;; promote string to bytes

(defmethod base:coerce
    ((object string) (schema (eql 'schema:bytes)))
  (declare (ignore schema))
  (babel:string-to-octets object :encoding :utf-8))

;;; promote bytes to string

(defmethod base:coerce
    ((object vector) (schema (eql 'schema:string)))
  (declare (ignore schema))
  (check-type object schema:bytes)
  (babel:octets-to-string object :encoding :utf-8))
