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
(defpackage #:cl-avro.resolution.logical
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:base #:cl-avro.resolution.base))
  (:import-from #:cl-avro.underlying
                #:to-underlying
                #:from-underlying))
(in-package #:cl-avro.resolution.logical)

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (symbol) (values symbol &optional)) find-schema-name))
  (defun find-schema-name (name)
    (multiple-value-bind (schema-name status)
        (find-symbol (string name) 'schema)
      (unless (eq status :external)
        (error "~S does not name a schema" name))
      schema-name))

  (declaim
   (ftype (function (schema:primitive-schema) (values symbol &optional))
          primitive-specializer))
  (defun primitive-specializer (schema)
    (ecase schema
      (schema:null 'null)
      (schema:boolean 'symbol)
      ((schema:int schema:long) 'integer)
      (schema:float 'single-float)
      (schema:double 'double-float)
      (schema:bytes 'vector)
      (schema:string 'string))))

(defmacro defunderlying (schema)
  (declare (symbol schema))
  (let* ((schema-name (find-schema-name schema))
         (metaclass-name (class-name (class-of (find-class schema-name))))
         (underlying (schema:underlying (find-class schema-name)))
         (underlying-specializer (primitive-specializer underlying)))
    (check-type underlying schema:primitive-schema)
    `(progn
       (defmethod base:coerce
           ((object ,schema-name) (schema (eql ',underlying)))
         (declare (ignore schema))
         (to-underlying object))

       (defmethod base:coerce
           ((object ,underlying-specializer) (schema ,metaclass-name))
         (from-underlying schema object))

       (defmethod base:coerce
           ((object ,schema-name) schema)
         (base:coerce (to-underlying object) schema))

       (defmethod base:coerce
           (object (schema ,metaclass-name))
         (from-underlying schema (base:coerce object ',underlying))))))

;; TODO maybe specialize change-class instead of creating a new instance
(defmacro deffrom (to (&rest from) &body initargs)
  (declare (symbol to))
  (let* ((to (find-schema-name to))
         (metaclass-name (class-name (class-of (find-class to))))
         (from (mapcar #'find-schema-name from))
         (initargs (subst 'object '% initargs :test #'eq)))
    (flet ((defcoerce (writer-class)
             `(defmethod base:coerce
                  ((object ,writer-class) (schema ,metaclass-name))
                (declare (ignore schema))
                (make-instance ',to ,@initargs))))
      `(progn
         ,@(mapcar #'defcoerce from)))))

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

(defmethod base:coerce
    ((object schema:decimal-object) (schema schema:decimal))
  (let* ((writer (class-of object))
         (writer-scale (schema:scale writer))
         (writer-precision (schema:precision writer))
         (reader-scale (schema:scale schema))
         (reader-precision (schema:precision schema)))
    (declare ((integer 0) reader-scale writer-scale)
             ((integer 1) reader-precision writer-precision))
    (unless (= reader-scale writer-scale)
      (error "Reader's decimal scale of ~S does not match writer's ~S"
             reader-scale writer-scale))
    (unless (= reader-precision writer-precision)
      (error "Reader's decimal precision of ~S does not match writer's ~S"
             reader-precision writer-precision))
    (change-class object schema)))

;;; duration schema

(defmethod base:coerce
    ((object schema:duration-object) (schema schema:duration))
  (change-class object schema))
