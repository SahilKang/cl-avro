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
(defpackage #:cl-avro.io.resolution.logical
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:assert-match
                #:make-resolver)
  (:import-from #:cl-avro.io.underlying
                #:to-underlying
                #:from-underlying)
  (:export #:assert-match
           #:make-resolver))
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
         `(eql ',(second type))))))

  (declaim
   (ftype (function ((or cons symbol) (or cons symbol))
                    (values cons &optional))
          defassert))
  (defun defassert (reader writer)
    `(defmethod assert-match
         ((reader ,reader) (writer ,writer))
       (declare (ignore reader writer))
       (values)))

  (declaim
   (ftype (function ((or cons symbol)
                     (or cons symbol)
                     (or (eql from-underlying) (eql to-underlying)))
                    (values cons &optional))
          defresolver))
  (defun defresolver (reader writer from/to)
    `(defmethod make-resolver
         ((reader ,reader) (writer ,writer))
       (declare (ignore writer))
       ,(ecase from/to
          (from-underlying '(lambda (writer-object)
                             (from-underlying reader writer-object)))
          (to-underlying '#'to-underlying)))))

(defmacro defunderlying (schema)
  (declare (symbol schema))
  (let* ((class (find-class (find-symbol (string schema) 'schema)))
         (metaclass-name (class-name (class-of class)))
         (underlying (schema:underlying class))
         (underlying-type (underlying-type (class-of class))))
    (unless (subtypep (class-of class) 'schema:logical-schema)
      (error "~S does not name a logical-schema" schema))
    `(progn
       ,(defassert metaclass-name metaclass-name)

       ,(defassert metaclass-name underlying-type)
       ,(defresolver metaclass-name underlying-type 'from-underlying)

       ,(defassert underlying-type metaclass-name)
       ,(defresolver underlying-type metaclass-name 'to-underlying)

       (defmethod assert-match
           ((reader ,metaclass-name) writer)
         (declare (ignore reader))
         (assert-match ',underlying writer))

       (defmethod make-resolver
           ((reader ,metaclass-name) writer)
         (let ((resolve (make-resolver ',underlying writer)))
           (lambda (writer-object)
             (from-underlying reader (funcall resolve writer-object)))))

       (defmethod assert-match
           (reader (writer ,metaclass-name))
         (declare (ignore writer))
         (assert-match reader ',underlying))

       (defmethod make-resolver
           (reader (writer ,metaclass-name))
         (declare (ignore writer))
         (let ((resolve (make-resolver reader ',underlying)))
           (lambda (writer-object)
             (funcall resolve (to-underlying writer-object))))))))

(eval-when (:compile-toplevel)
  (declaim (ftype (function (symbol) (values symbol &optional)) find-metaclass))
  (defun find-metaclass (symbol)
    (multiple-value-bind (metaclass status)
        (find-symbol (format nil "~A-SCHEMA" symbol) 'schema)
      (unless (eq status :external)
        (error "~S does not name a metaclass" symbol))
      metaclass))

  (declaim (ftype (function (symbol) (values symbol &optional)) find-schema))
  (defun find-schema (symbol)
    (multiple-value-bind (schema status)
        (find-symbol (string symbol) 'schema)
      (unless (eq status :external)
        (error "~S does not name a schema" symbol))
      schema)))

(defmacro deffrom (to (&rest from) &body initargs)
  (declare (symbol to))
  (let ((reader-class (find-metaclass to))
        (reader-schema (find-schema to)))
    (flet ((defresolve (arg writer-class)
               (declare (symbol arg writer-class))
             `(defmethod make-resolver
                  ((reader ,reader-class) (writer ,writer-class))
                (declare (ignore reader writer))
                (lambda (,arg)
                  (make-instance
                   ',reader-schema
                   ,@(subst arg '% initargs :test #'eq))))))
      `(progn
         ,@(loop
             for writer in from
             for writer-class = (find-metaclass writer)
             collect (defassert reader-class writer-class)
             collect (defresolve writer writer-class))))))

;;; uuid schema

(defunderlying uuid)

;;; date schema

(defunderlying date)

(deffrom date
    (timestamp-millis
     timestamp-micros
     local-timestamp-millis
     local-timestamp-micros)
  :year (schema:year %)
  :month (schema:month %)
  :day (schema:day %))

;;; time-millis schema

(defunderlying time-millis)

(deffrom time-millis
    (time-micros
     timestamp-millis
     timestamp-micros
     local-timestamp-millis
     local-timestamp-micros)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :millisecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (truncate
                  (+ (* 1000 second)
                     (* 1000 remainder)))))

;;; time-micros schema

(defunderlying time-micros)

(deffrom time-micros
    (time-millis
     timestamp-millis
     timestamp-micros
     local-timestamp-millis
     local-timestamp-micros)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :microsecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (+ (* 1000 1000 second)
                    (* 1000 1000 remainder))))

;;; timestamp-millis schema

(defunderlying timestamp-millis)

(deffrom timestamp-millis
    (timestamp-micros)
  :year (schema:year %)
  :month (schema:month %)
  :day (schema:day %)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :millisecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (truncate
                  (+ (* 1000 second)
                     (* 1000 remainder)))))

;;; timestamp-micros schema

(defunderlying timestamp-micros)

(deffrom timestamp-micros
    (timestamp-millis)
  :year (schema:year %)
  :month (schema:month %)
  :day (schema:day %)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :microsecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (+ (* 1000 1000 second)
                    (* 1000 1000 remainder))))

;;; local-timestamp-millis schema

(defunderlying local-timestamp-millis)

(deffrom local-timestamp-millis
    (local-timestamp-micros)
  :year (schema:year %)
  :month (schema:month %)
  :day (schema:day %)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :millisecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (truncate
                  (+ (* 1000 second)
                     (* 1000 remainder)))))

;;; local-timestamp-micros schema

(defunderlying local-timestamp-micros)

(deffrom local-timestamp-micros
    (local-timestamp-millis)
  :year (schema:year %)
  :month (schema:month %)
  :day (schema:day %)
  :hour (schema:hour %)
  :minute (schema:minute %)
  :microsecond (multiple-value-bind (second remainder)
                   (schema:second %)
                 (+ (* 1000 1000 second)
                    (* 1000 1000 remainder))))

;;; decimal schema

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

(defmethod make-resolver
    ((reader schema:decimal) (writer schema:decimal))
  (declare (ignore writer))
  (lambda (writer-decimal)
    (make-instance reader :unscaled (schema:unscaled writer-decimal))))

;;; duration schema

(defmethod assert-match
    ((reader schema:duration) (writer schema:duration))
  (declare (ignore reader writer))
  (values))

(defmethod make-resolver
    ((reader schema:duration) (writer schema:duration))
  (declare (ignore writer))
  (lambda (writer-duration)
    (make-instance
     reader
     :months (schema:months writer-duration)
     :days (schema:days writer-duration)
     :milliseconds (schema:milliseconds writer-duration))))
