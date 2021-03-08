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
(defpackage #:cl-avro.schema.io.st-json
  (:use #:cl)
  (:import-from #:cl-avro.schema.primitive
                #:true
                #:false
                #:bytes)
  (:import-from #:cl-avro.schema.complex
                #:array-object
                #:map-object
                #:generic-hash-table-count
                #:hashmap
                #:fixed-object
                #:raw-buffer
                #:record-object
                #:fields
                #:name
                #:union-object
                #:object)
  (:export #:convert-from-st-json
           #:convert-to-st-json))
(in-package #:cl-avro.schema.io.st-json)

;;; convert-from-st-json

(defgeneric convert-from-st-json (value)
  (:method (value)
    value))

(defmethod convert-from-st-json ((null (eql :null)))
  (declare (ignore null))
  nil)

(defmethod convert-from-st-json ((boolean (eql :true)))
  (declare (ignore boolean))
  'true)

(defmethod convert-from-st-json ((boolean (eql :false)))
  (declare (ignore boolean))
  'false)

(defmethod convert-from-st-json ((list list))
  (map 'simple-vector #'convert-from-st-json list))

(defmethod convert-from-st-json ((json-object st-json:jso))
  (let ((hash-table (make-hash-table :test #'equal)))
    (flet ((convert (key value)
             (setf (gethash key hash-table)
                   (convert-from-st-json value))))
      (st-json:mapjso #'convert json-object))
    hash-table))

;;; convert-to-st-json

(defgeneric convert-to-st-json (value)
  (:method (value)
    value))

(defmethod convert-to-st-json ((null null))
  (declare (ignore null))
  :null)

(defmethod convert-to-st-json ((boolean (eql 'true)))
  (declare (ignore boolean))
  :true)

(defmethod convert-to-st-json ((boolean (eql 'false)))
  (declare (ignore boolean))
  :false)

(defmethod convert-to-st-json ((array array))
  (if (typep array 'bytes)
      (babel:octets-to-string array :encoding :latin-1)
      array))

(defmethod convert-to-st-json ((fixed fixed-object))
  (babel:octets-to-string (raw-buffer fixed) :encoding :latin-1))

(defmethod convert-to-st-json ((array array-object))
  (map 'simple-vector #'convert-to-st-json array))

(defmethod convert-to-st-json ((map map-object))
  (let ((converted (make-hash-table
                    :test #'equal :size (generic-hash-table-count map))))
    (flet ((convert (key value)
             (setf (gethash key converted)
                   (convert-to-st-json value))))
      (hashmap #'convert map))
    converted))

(defmethod convert-to-st-json ((record record-object))
  (loop
    with fields = (fields (class-of record))
    with hash-table = (make-hash-table :test #'equal :size (length fields))

    for field across fields
    for (name slot-name) = (multiple-value-list (name field))
    for value = (slot-value record slot-name)

    do (setf (gethash name hash-table)
             (convert-to-st-json value))

    finally
       (return hash-table)))

(defmethod convert-to-st-json ((union union-object))
  (convert-to-st-json (object union)))
