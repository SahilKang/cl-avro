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
(defpackage #:cl-avro.io.resolution.promoted
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:assert-match
                #:make-resolver)
  (:export #:assert-match
           #:make-resolver))
(in-package #:cl-avro.io.resolution.promoted)

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (symbol) (values schema:primitive-schema &optional))
          find-schema))
  (defun find-schema (symbol)
    (let ((schema (find-symbol (string symbol) 'schema)))
      (check-type schema schema:primitive-schema)
      schema)))

(defmacro promote (from to &body body)
  (let ((from (find-schema from))
        (to (find-schema to)))
    `(progn
       (defmethod assert-match
           ((reader (eql ',to)) (writer (eql ',from)))
         (declare (ignore reader writer))
         (values))

       (defmethod make-resolver
           ((reader (eql ',to)) (writer (eql ',from)))
         (declare (ignore reader writer))
         ,@body))))

;;; int to long, float, or double

(promote int long
  #'identity)

(promote int float
  (lambda (writer-int)
    (coerce writer-int 'schema:float)))

(promote int double
  (lambda (writer-int)
    (coerce writer-int 'schema:double)))

;;; long to float or double

(promote long float
  (lambda (writer-long)
    (coerce writer-long 'schema:float)))

(promote long double
  (lambda (writer-long)
    (coerce writer-long 'schema:double)))

;;; float to double

(promote float double
  (lambda (writer-float)
    (coerce writer-float 'schema:double)))

;;; string to bytes

(promote string bytes
  (lambda (writer-string)
    (babel:string-to-octets writer-string :encoding :utf-8)))

;;; bytes to string

(promote bytes string
  (lambda (writer-bytes)
    (babel:octets-to-string writer-bytes :encoding :utf-8)))
