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
(defpackage #:cl-avro.io.resolution.logical
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from
                #:resolve
                #:assert-match)
  (:import-from #:cl-avro.io.resolution.resolve
                #:resolved
                #:reader
                #:writer)
  (:export #:deserialize-from-vector
           #:deserialize-from-stream
           #:resolve
           #:assert-match))
(in-package #:cl-avro.io.resolution.logical)

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (class) (values (or cons symbol) &optional))
          underlying-type))
  (defun underlying-type (logical-schema)
    (let* ((slots (closer-mop:class-direct-slots logical-schema))
           (underlying (find 'schema:underlying slots
                             :key #'closer-mop:slot-definition-name
                             :test #'eq))
           (type (closer-mop:slot-definition-type underlying)))
      (etypecase type
        (symbol type)
        (list
         (unless (eq (first type) 'eql)
           (error "Expected eql specifier: ~S" type))
         `(eql ',(second type)))))))

(defmacro defunderlying (schema)
  (declare (symbol schema))
  (let* ((class (find-class (find-symbol (string schema) 'schema)))
         (class-name (class-name class))
         (underlying (underlying-type class)))
    (unless (subtypep class 'schema:logical-schema)
      (error "~S does not name a logical-schema" schema))
    `(progn
       (defmethod assert-match
           ((reader ,class-name) (writer ,class-name))
         (declare (ignore reader writer))
         (values))

       (defmethod assert-match
           ((reader ,class-name) (writer ,underlying))
         (declare (ignore reader writer))
         (values))

       (defmethod assert-match
           ((reader ,underlying) (writer ,class-name))
         (declare (ignore reader writer))
         (values)))))

;; uuid schema

(defunderlying uuid-schema)

;; date schema

(defunderlying date-schema)

(defclass resolved-date (resolved)
  ((reader
    :type schema:date-schema)
   (writer
    :type (or schema:timestamp-millis-schema
              schema:timestamp-micros-schema))))

(defmethod assert-match
    ((reader schema:date-schema) (writer schema:timestamp-millis-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:date-schema) (writer schema:timestamp-millis-schema))
  (make-instance 'resolved-date :reader reader :writer writer))

(defmethod assert-match
    ((reader schema:date-schema) (writer schema:timestamp-micros-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:date-schema) (writer schema:timestamp-micros-schema))
  (make-instance 'resolved-date :reader reader :writer writer))

(define-deserialize-from resolved-date
  `(multiple-value-bind (timestamp bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :year (schema:year timestamp)
       :month (schema:month timestamp)
       :day (schema:day timestamp))
      bytes-read)))

;; time-millis schema

(defunderlying time-millis-schema)

(defclass resolved-time-millis (resolved)
  ((reader
    :type schema:time-millis-schema)
   (writer
    :type schema:time-micros-schema)))

(defmethod assert-match
    ((reader schema:time-millis-schema) (writer schema:time-micros-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:time-millis-schema) (writer schema:time-micros-schema))
  (make-instance 'resolved-time-millis :reader reader :writer writer))

(declaim
 (ftype (function (local-time:timestamp) (values (integer 0 59999) &optional))
        get-millisecond)
 (inline get-millisecond))
(defun get-millisecond (timestamp)
  (let ((second (local-time:timestamp-second timestamp))
        (millisecond (local-time:timestamp-millisecond timestamp)))
    (declare ((integer 0 59) second)
             ((integer 0 999) millisecond))
    (+ (* second 1000) millisecond)))
(declaim (notinline get-millisecond))

(define-deserialize-from resolved-time-millis
  (declare (inline get-millisecond))
  `(multiple-value-bind (time-micros bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :hour (schema:hour time-micros)
       :minute (schema:minute time-micros)
       :millisecond (get-millisecond time-micros))
      bytes-read)))

;; time-micros schema

(defunderlying time-micros-schema)

(defclass resolved-time-micros (resolved)
  ((reader
    :type schema:time-micros-schema)
   (writer
    :type schema:time-millis-schema)))

(defmethod assert-match
    ((reader schema:time-micros-schema) (writer schema:time-millis-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:time-micros-schema) (writer schema:time-millis-schema))
  (make-instance 'resolved-time-micros :reader reader :writer writer))

(declaim
 (ftype (function (local-time:timestamp)
                  (values (integer 0 59999999) &optional))
        get-microsecond)
 (inline get-microsecond))
(defun get-microsecond (timestamp)
  (let ((second (local-time:timestamp-second timestamp))
        (microsecond (local-time:timestamp-microsecond timestamp)))
    (declare ((integer 0 59) second)
             ((integer 0 999999) microsecond))
    (+ (* second 1000 1000) microsecond)))
(declaim (notinline get-microsecond))

(define-deserialize-from resolved-time-micros
  (declare (inline get-microsecond))
  `(multiple-value-bind (time-millis bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :hour (schema:hour time-millis)
       :minute (schema:minute time-millis)
       :microsecond (get-microsecond time-millis))
      bytes-read)))

;; timestamp-millis schema

(defunderlying timestamp-millis-schema)

(defclass resolved-timestamp-millis (resolved)
  ((reader
    :type schema:timestamp-millis-schema)
   (writer
    :type schema:timestamp-micros-schema)))

(defmethod assert-match
    ((reader schema:timestamp-millis-schema)
     (writer schema:timestamp-micros-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:timestamp-millis-schema)
     (writer schema:timestamp-micros-schema))
  (make-instance 'resolved-timestamp-millis :reader reader :writer writer))

(define-deserialize-from resolved-timestamp-millis
  (declare (inline get-millisecond))
  `(multiple-value-bind (timestamp-micros bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :year (schema:year timestamp-micros)
       :month (schema:month timestamp-micros)
       :day (schema:day timestamp-micros)
       :hour (schema:hour timestamp-micros)
       :minute (schema:minute timestamp-micros)
       :millisecond (get-millisecond timestamp-micros))
      bytes-read)))

;; timestamp-micros schema

(defunderlying timestamp-micros-schema)

(defclass resolved-timestamp-micros (resolved)
  ((reader
    :type schema:timestamp-micros-schema)
   (writer
    :type schema:timestamp-millis-schema)))

(defmethod assert-match
    ((reader schema:timestamp-micros-schema)
     (writer schema:timestamp-millis-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:timestamp-micros-schema)
     (writer schema:timestamp-millis-schema))
  (make-instance 'resolved-timestamp-micros :reader reader :writer writer))

(define-deserialize-from resolved-timestamp-micros
  (declare (inline get-microsecond))
  `(multiple-value-bind (timestamp-millis bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :year (schema:year timestamp-millis)
       :month (schema:month timestamp-millis)
       :day (schema:day timestamp-millis)
       :hour (schema:hour timestamp-millis)
       :minute (schema:minute timestamp-millis)
       :microsecond (get-microsecond timestamp-millis))
      bytes-read)))

;; local-timestamp-millis schema

(defunderlying local-timestamp-millis-schema)

(defclass resolved-local-timestamp-millis (resolved)
  ((reader
    :type schema:local-timestamp-millis-schema)
   (writer
    :type schema:local-timestamp-micros-schema)))

(defmethod assert-match
    ((reader schema:local-timestamp-millis-schema)
     (writer schema:local-timestamp-micros-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
  ((reader schema:local-timestamp-millis-schema)
   (writer schema:local-timestamp-micros-schema))
  (make-instance 'resolved-local-timestamp-millis
                 :reader reader :writer writer))

(define-deserialize-from resolved-local-timestamp-millis
  (declare (inline get-millisecond))
  `(multiple-value-bind (local-timestamp-micros bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :year (schema:year local-timestamp-micros)
       :month (schema:month local-timestamp-micros)
       :day (schema:day local-timestamp-micros)
       :hour (schema:hour local-timestamp-micros)
       :minute (schema:minute local-timestamp-micros)
       :millisecond (get-millisecond local-timestamp-micros))
      bytes-read)))

;; local-timestamp-micros schema

(defunderlying local-timestamp-micros-schema)

(defclass resolved-local-timestamp-micros (resolved)
  ((reader
    :type schema:local-timestamp-micros-schema)
   (writer
    :type schema:local-timestamp-millis-schema)))

(defmethod assert-match
    ((reader schema:local-timestamp-micros-schema)
     (writer schema:local-timestamp-millis-schema))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:local-timestamp-micros-schema)
     (writer schema:local-timestamp-millis-schema))
  (make-instance 'resolved-local-timestamp-micros
                 :reader reader :writer writer))

(define-deserialize-from resolved-local-timestamp-micros
  (declare (inline get-microsecond))
  `(multiple-value-bind (local-timestamp-millis bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :year (schema:year local-timestamp-millis)
       :month (schema:month local-timestamp-millis)
       :day (schema:day local-timestamp-millis)
       :hour (schema:hour local-timestamp-millis)
       :minute (schema:minute local-timestamp-millis)
       :microsecond (get-microsecond local-timestamp-millis))
      bytes-read)))

;; decimal schema

(defclass resolved-decimal (resolved)
  ((reader
    :type schema:decimal)
   (writer
    :type schema:decimal)))

(defmethod assert-match
    ((reader schema:decimal) (writer schema:decimal))
  (let ((reader-scale (schema:scale reader))
        (writer-scale (schema:scale writer))
        (reader-precision (schema:precision reader))
        (writer-precision (schema:precision writer)))
    (declare ((integer 0) reader-scale writer-scale)
             ((integer 1) reader-precision writer-precision))
    (unless (= reader-scale writer-scale)
      (error "Reader's decimal scale of ~S does not match writer's ~S"
             reader-scale writer-scale))
    (unless (= reader-precision writer-precision)
      (error "Reader's decimal precision of ~S does not match writer's ~S"
             reader-precision writer-precision))))

(defmethod resolve
    ((reader schema:decimal) (writer schema:decimal))
  (make-instance 'resolved-decimal :reader reader :writer writer))

(define-deserialize-from resolved-decimal
  `(multiple-value-bind (decimal bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :unscaled (schema:unscaled decimal))
      bytes-read)))

;; duration schema

(defclass resolved-duration (resolved)
  ((reader
    :type schema:duration)
   (writer
    :type schema:duration)))

(defmethod assert-match
    ((reader schema:duration) (writer schema:duration))
  (declare (ignore reader writer))
  (values))

(defmethod resolve
    ((reader schema:duration) (writer schema:duration))
  (make-instance 'resolved-duration :reader reader :writer writer))

(define-deserialize-from resolved-duration
  `(multiple-value-bind (duration bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (values
      (make-instance
       (reader schema)
       :months (schema:months duration)
       :days (schema:days duration)
       :milliseconds (schema:milliseconds duration))
      bytes-read)))
