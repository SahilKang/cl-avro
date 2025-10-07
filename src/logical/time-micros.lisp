;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.time-micros
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:cl-avro.internal.logical.datetime
                #:local-timezone
                #:local-hour-minute
                #:local-second-micros)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:cl-avro.internal.time-micros)

;;; time-micros

(defclass api:time-micros
    (api:logical-object local-hour-minute local-second-micros)
  ()
  (:metaclass api:logical-schema)
  (:documentation
   "Avro time-micros schema.

This represents a microsecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:time-micros))))
  (declare (ignore schema))
  'api:long)

(defmethod initialize-instance :after
    ((instance api:time-micros) &key hour minute microsecond)
  (when (or hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour 1 1 1
       :into instance :timezone (local-timezone instance)))))

;;; to/from-underlying

(deftype nonnegative-long ()
  '(and api:long (integer 0)))

(declaim
 (ftype (function (api:time-micros) (values nonnegative-long &optional))
        to-underlying))
(defun to-underlying (time-micros)
  "Serialized as the number of microseconds after midnight, 00:00:00.000000."
  (let ((hour (api:hour time-micros))
        (minute (api:minute time-micros))
        (microsecond (api:microsecond time-micros)))
    (declare ((mod 60) hour minute)
             ((mod 60000000) microsecond))
    (+ (* hour 60 60 1000 1000)
       (* minute 60 1000 1000)
       microsecond)))

(declaim
 (ftype (function (nonnegative-long) (values api:time-micros &optional))
        from-underlying))
(defun from-underlying (microseconds-after-midnight)
  (multiple-value-bind (hour microseconds-after-midnight)
      (truncate microseconds-after-midnight (* 60 60 1000 1000))
    (multiple-value-bind (minute microseconds-after-midnight)
        (truncate microseconds-after-midnight (* 60 1000 1000))
      (make-instance
       'api:time-micros
       :hour hour
       :minute minute
       :microsecond microseconds-after-midnight))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:time-micros))))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object api:time-micros))
  (api:serialized-size (to-underlying object)))

;;; serialize

(defmethod internal:serialize
    ((object api:time-micros) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (internal:serialize (to-underlying object) into :start start))

(defmethod internal:serialize
    ((object api:time-micros) (into stream) &key)
  (internal:serialize (to-underlying object) into))

(defmethod api:serialize
    ((object api:time-micros)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema (eql (find-class 'api:time-micros)))
     (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (microseconds-after-midnight bytes-read)
      (api:deserialize 'api:long input :start start)
    (values (from-underlying microseconds-after-midnight) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:time-micros))) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (microseconds-after-midnight bytes-read)
      (api:deserialize 'api:long input)
    (values (from-underlying microseconds-after-midnight) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:time-micros))) (input vector)
     &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:long input start))

(defmethod internal:skip
    ((schema (eql (find-class 'api:time-micros))) (input stream)
     &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:long input))

(defmethod api:compare
    ((schema (eql (find-class 'api:time-micros))) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:long left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:time-micros)))
     (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:long left right))

;;; coerce

(defmethod api:coerce
    ((object api:time-micros) (schema (eql (find-class 'api:time-micros))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:time-micros) (schema (eql 'api:long)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:time-micros) (schema (eql 'api:float)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:float))

(defmethod api:coerce
    ((object api:time-micros) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql (find-class 'api:time-micros))))
  (declare (ignore schema)
           (nonnegative-long object))
  (from-underlying object))

(defmethod api:coerce
    ((object api:time-millis) (schema (eql (find-class 'api:time-micros))))
  (declare (ignore schema))
  (change-class object 'api:time-micros))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:time-micros))
  (to-underlying default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:time-micros))) (default integer))
  (declare (ignore schema)
           (nonnegative-long default))
  (from-underlying default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "long"
                    "logicalType" "time-micros"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:time-micros)))
