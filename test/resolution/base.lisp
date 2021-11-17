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
(defpackage #:cl-avro/test/resolution/base
  (:use #:cl)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:export #:find-schema
           #:bytes
           #:initarg-for-millis/micros
           #:millisecond
           #:microsecond))
(in-package #:cl-avro/test/resolution/base)

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (symbol) (values symbol &optional)) find-schema))
  (defun find-schema (name)
    (multiple-value-bind (schema status)
        (find-symbol (string name) 'avro)
      (unless (eq status :external)
        (error "~S does not name a schema" name))
      (unless (typep schema 'avro:primitive-schema)
        (check-type (find-class schema) avro:schema))
      schema))

  (declaim (ftype (function (string) (values string &optional)) suffix))
  (defun suffix (string)
    (let ((last-hyphen (position #\- string :from-end t :test #'char=)))
      (unless last-hyphen
        (error "~S does not contain a hyphen" string))
      (subseq string (1+ last-hyphen))))

  (declaim
   (ftype (function (symbol)
                    (values (or (eql :millisecond) (eql :microsecond)) &optional))
          initarg-for-millis/micros))
  (defun initarg-for-millis/micros (symbol)
    (let ((suffix (suffix (string symbol))))
      (cond
        ((string= suffix "MILLIS") :millisecond)
        ((string= suffix "MICROS") :microsecond)
        (t (error "~S is neither MILLIS or MICROS" suffix))))))

(declaim
 (ftype (function (&rest (unsigned-byte 8))
                  (values (simple-array (unsigned-byte 8) (*)) &optional))
        bytes))
(defun bytes (&rest bytes)
  (make-array (length bytes) :element-type '(unsigned-byte 8) :initial-contents bytes))

(declaim
 (ftype (function (local-time:timestamp) (values avro:int &optional))
        millisecond))
(defun millisecond (time)
  (multiple-value-bind (second remainder)
      (avro:second time)
    (nth-value 0 (truncate
                  (+ (* 1000 second)
                     (* 1000 remainder))))))

(declaim
 (ftype (function (local-time:timestamp) (values avro:long &optional))
        microsecond))
(defun microsecond (time)
  (multiple-value-bind (second remainder)
      (avro:second time)
    (+ (* 1000 1000 second)
       (* 1000 1000 remainder))))
