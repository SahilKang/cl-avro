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

(defclass logical-schema ()
  ((underlying-schema
    :reader underlying-schema
    :type avro-schema
    :documentation "Underlying avro schema.")))


(defclass decimal-schema (logical-schema)
  ((underlying-schema
    :type (or (eql bytes-schema) fixed-schema))
   (scale
    :reader scale
    :type (integer 0)
    :documentation "Decimal scale.")
   (precision
    :reader precision
    :type (integer 1)
    :documentation "Decimal precision"))
  (:documentation
   "Represents an avro decimal schema."))

(defun assert-decent-precision (precision size)
  (declare (integer precision size))
  (unless (> size 0)
    (error "fixed-schema with size ~S is not greater than 0" size))
  (let ((max-precision (floor
                        (log (1- (expt 2 (1- (* 8 size))))
                             10))))
    (when (> precision max-precision)
      (error "fixed-schema with size ~S can store up to ~S digits, not ~S"
             size
             max-precision
             precision))))

(defmethod initialize-instance :after
    ((decimal-schema decimal-schema)
     &key
       (underlying-schema (error "Must supply :underlying-schema"))
       (scale 0)
       (precision (error "Must supply :precision")))
  (check-type scale (integer 0))
  (check-type precision (integer 1))
  (check-type underlying-schema (or (eql bytes-schema) fixed-schema))
  (unless (<= scale precision)
    (error "Scale ~S cannot be greater than precision ~S" scale precision))
  (when (typep underlying-schema 'fixed-schema)
    (assert-decent-precision precision (size underlying-schema)))
  (with-slots ((us underlying-schema) (s scale) (p precision)) decimal-schema
    (setf us underlying-schema
          s scale
          p precision)))


(declaim (inline hexp))
(defun hexp (char)
  (declare (character char)
           (optimize (speed 3) (safety 0))
           (inline digit-p))
  "Determine if CHAR conforms to /[0-9a-z-A-Z]/"
  (let ((char-code (char-code char)))
    (or (digit-p char-code)
        (in-range-p char-code #\a #\f)
        (in-range-p char-code #\A #\F))))
(declaim (notinline hexp))

(declaim (inline rfc-4122-uuid-p))
(defun rfc-4122-uuid-p (string)
  (declare (string-schema string)
           (optimize (speed 3) (safety 0))
           (inline hexp))
  "Determine if STRING conforms to RFC-4122."
  (and (= 36 (length string))
       (char= #\-
              (schar string 8)
              (schar string 13)
              (schar string 18)
              (schar string 23))
       (loop for i from 00 below 08 always (hexp (schar string i)))
       (loop for i from 09 below 13 always (hexp (schar string i)))
       (loop for i from 14 below 18 always (hexp (schar string i)))
       (loop for i from 19 below 23 always (hexp (schar string i)))
       (loop for i from 24 below 36 always (hexp (schar string i)))))
(declaim (notinline rfc-4122-uuid-p))

(defun uuid-schema-p (string)
  (declare (optimize (speed 3) (safety 0))
           (inline rfc-4122-uuid-p))
  (and (typep string 'string-schema)
       (rfc-4122-uuid-p string)))

(deftype uuid-schema ()
  "Represents the avro uuid schema."
  '(satisfies uuid-schema-p))
