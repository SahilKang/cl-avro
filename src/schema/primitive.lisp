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

(in-package #:cl-avro)

;;; avro primitive schemas

(eval-when (:compile-toplevel :execute)
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

(defgeneric assert-valid (schema object)
  (:method (schema object)
    (declare (optimize (speed 3) (safety 0))))

  (:documentation
   "Signal a condition if OBJECT is not valid according to SCHEMA."))

;; null-schema

(defprimitive null-schema null
  "Represents the avro null schema.")

;; boolean-schema

(defprimitive boolean-schema boolean
  "Represents the avro boolean schema.")

;; int-schema

(defprimitive int-schema (signed-byte 32)
  "Represents the avro int schema.")

(defmethod assert-valid ((schema (eql 'int-schema)) (integer integer))
  "Valid if INTEGER is a signed 32-bit integer."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (check-type integer int-schema "a signed 32-bit integer"))

;; long-schema

(defprimitive long-schema (signed-byte 64)
  "Represents the avro long schema.")

(defmethod assert-valid ((schema (eql 'long-schema)) (integer integer))
  "Valid if INTEGER is a signed 64-bit integer."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (check-type integer long-schema "a signed 64-bit integer"))

;; float-schema

(defprimitive float-schema
    #.(let ((float-digits (float-digits 1f0)))
        (unless (= 24 float-digits)
          (error "(float-digits 1f0) is ~S and not 24" float-digits))
        'single-float)
  "Represents the avro float schema.")

;; double-schema

(defprimitive double-schema
    #.(let ((float-digits (float-digits 1d0)))
        (unless (= 53 float-digits)
          (error "(float-digits 1d0) is ~S and not 53" float-digits))
        'double-float)
  "Represents the avro double schema.")

;; bytes-schema

(defarray byte (unsigned-byte 8))

(defprimitive bytes-schema array[byte]
  "Represents the avro bytes schema.")

(defmethod assert-valid ((schema (eql 'bytes-schema)) (array simple-array))
  "Valid if ARRAY is a simple-array of unsigned 8-bit bytes."
  (declare (ignore schema)
           (optimize (speed 3) (safety 0)))
  (check-type array array[byte]))

;; string-schema

(defprimitive string-schema simple-string
  "Represents the avro string schema.")


(deftype primitive-schema ()
  `(member ,@*primitives*))

;;; avro logical schemas aliasing primitives

(eval-when (:compile-toplevel :execute)
  (defparameter *logical-aliases* nil
    "Alist mapping a logical schema to the primitive schema it aliases."))

(defmacro defalias (name (primitive &rest moar) &body docstring)
  "Define a logical schema NAME aliasing PRIMITIVE with MOAR restrictions."
  (declare (symbol name)
           (primitive-schema primitive))
  (pushnew (cons name primitive) *logical-aliases* :test #'eq :key #'car)
  `(deftype ,name ()
     ,@docstring
     ',(if moar
           `(and ,primitive ,@moar)
           primitive)))

;; uuid-schema

(defun! hexp (char)
    ((character) boolean)
  "Determine if CHAR conforms to /[0-9a-z-A-Z]/"
  (declare (inline digit-p))
  (let ((char-code (char-code char)))
    (or (digit-p char-code)
        (in-range-p char-code #\a #\f)
        (in-range-p char-code #\A #\F))))

(defun! rfc-4122-uuid-p (uuid)
    ((string-schema) boolean)
  "Determine if UUID conforms to RFC-4122."
  (declare (inline hexp))
  (and (= 36 (length uuid))
       (char= #\-
              (schar uuid 8)
              (schar uuid 13)
              (schar uuid 18)
              (schar uuid 23))
       (loop for i from 00 below 08 always (hexp (schar uuid i)))
       (loop for i from 09 below 13 always (hexp (schar uuid i)))
       (loop for i from 14 below 18 always (hexp (schar uuid i)))
       (loop for i from 19 below 23 always (hexp (schar uuid i)))
       (loop for i from 24 below 36 always (hexp (schar uuid i)))))

(defun! uuid-schema-p (uuid)
    ((t) boolean)
  (declare (inline rfc-4122-uuid-p))
  (and (typep uuid 'string-schema)
       (rfc-4122-uuid-p uuid)))

(defalias uuid-schema (string-schema (satisfies uuid-schema-p))
  "Represents the avro uuid schema.")

(defmethod assert-valid ((schema (eql 'uuid-schema)) (uuid simple-string))
  "UUID is valid if it conforms to RFC-4122."
  (declare (ignore schema)
           (inline rfc-4122-uuid-p)
           (optimize (speed 3) (safety 0)))
  (unless (rfc-4122-uuid-p uuid)
    (error "~S does not conform to RFC-4122" uuid)))

;; date-schema

(defalias date-schema (int-schema)
  "Represents the avro date schema.

This represents a date on the calendar, with no reference to a
particular timezone or time-of-day.

Serialized as the number of days from the ISO unix epoch 1970-01-01.")

;; time-millis-schema

(defalias time-millis-schema (int-schema (integer 0))
  "Represents the avro time-millis schema.

This represents a millisecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date.

Serialized as the number of milliseconds after midnight, 00:00:00.000.")

;; time-micros-schema

(defalias time-micros-schema (long-schema (integer 0))
  "Represents the avro time-micros schema.

This represents a microsecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date.

Serialized as the number of microseconds after midnight, 00:00:00.000000.")

;; timestamp-millis-schema

(defalias timestamp-millis-schema (long-schema)
  "Represents the avro timestamp-millis schema.

This represents a millisecond-precision instant on the global
timeline, independent of a particular timezone or calendar.

Serialized as the number of milliseconds from the UTC unix epoch 1970-01-01T00:00:00.000.")

;; timestamp-micros-schema

(defalias timestamp-micros-schema (long-schema)
  "Represents the avro timestamp-micros schema.

This represents a microsecond-precision instant on the global
timeline, independent of a particular timezone or calendar.

Serialized as the number of microseconds from the UTC unix epoch 1970-01-01T00:00:00.000000.")

;; local-timestamp-millis-schema

(defalias local-timestamp-millis-schema (long-schema)
  "Represents the avro local-timestamp-millis schema.

This represents a millisecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local.

Serialized as the number of milliseconds from 1970-01-01T00:00:00.000.")

;; local-timestamp-micros-schema

(defalias local-timestamp-micros-schema (long-schema)
  "Represents the avro local-timestamp-micros schema.

This represents a microsecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local.

Serialized as the number of microseconds from 1970-01-01T00:00:00.000000.")


(deftype logical-alias ()
  `(member ,@(mapcar #'car *logical-aliases*)))
