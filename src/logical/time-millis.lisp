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
(defpackage #:cl-avro.internal.time-millis
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
                #:local-second-millis)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:cl-avro.internal.time-millis)

;;; time-millis

(defclass api:time-millis
    (api:logical-object local-hour-minute local-second-millis)
  ()
  (:metaclass api:logical-schema)
  (:documentation
   "Avro time-millis schema.

This represents a millisecond-precision time-of-day, with no reference
to a particular calendar, timezone, or date."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:time-millis))))
  (declare (ignore schema))
  'api:int)

(defmethod initialize-instance :after
    ((instance api:time-millis) &key hour minute millisecond)
  (when (or hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour 1 1 1
       :into instance :timezone (local-timezone instance)))))

;;; to/from-underlying

(deftype nonnegative-int ()
  '(and api:int (integer 0)))

(declaim
 (ftype (function (api:time-millis) (values nonnegative-int &optional))
        to-underlying))
(defun to-underlying (time-millis)
  "Serialized as the number of milliseconds after midnight, 00:00:00.000."
  (let ((hour (api:hour time-millis))
        (minute (api:minute time-millis))
        (millisecond (api:millisecond time-millis)))
    (declare ((mod 60) hour minute)
             ((mod 60000) millisecond))
    (+ (* hour 60 60 1000)
       (* minute 60 1000)
       millisecond)))

(declaim
 (ftype (function (nonnegative-int) (values api:time-millis &optional))
        from-underlying))
(defun from-underlying (milliseconds-after-midnight)
  (multiple-value-bind (hour milliseconds-after-midnight)
      (truncate milliseconds-after-midnight (* 60 60 1000))
    (multiple-value-bind (minute milliseconds-after-midnight)
        (truncate milliseconds-after-midnight (* 60 1000))
      (make-instance
       'api:time-millis
       :hour hour
       :minute minute
       :millisecond milliseconds-after-midnight))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:time-millis))))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object api:time-millis))
  (api:serialized-size (to-underlying object)))

;;; serialize

(defmethod internal:serialize
    ((object api:time-millis) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (internal:serialize (to-underlying object) into :start start))

(defmethod internal:serialize
    ((object api:time-millis) (into stream) &key)
  (internal:serialize (to-underlying object) into))

(defmethod api:serialize
    ((object api:time-millis)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object)) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema (eql (find-class 'api:time-millis)))
     (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (milliseconds-after-midnight bytes-read)
      (api:deserialize 'api:int input :start start)
    (values (from-underlying milliseconds-after-midnight) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:time-millis))) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (milliseconds-after-midnight bytes-read)
      (api:deserialize 'api:int input)
    (values (from-underlying milliseconds-after-midnight) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:time-millis))) (input vector)
     &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:int input start))

(defmethod internal:skip
    ((schema (eql (find-class 'api:time-millis))) (input stream)
     &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:int input))

(defmethod api:compare
    ((schema (eql (find-class 'api:time-millis))) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:int left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:time-millis)))
     (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:int left right))

;;; coerce

(defmethod api:coerce
    ((object api:time-millis) (schema (eql (find-class 'api:time-millis))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:time-millis) (schema (eql 'api:int)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:time-millis) (schema (eql 'api:long)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:time-millis) (schema (eql 'api:float)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:float))

(defmethod api:coerce
    ((object api:time-millis) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql (find-class 'api:time-millis))))
  (declare (ignore schema)
           (nonnegative-int object))
  (from-underlying object))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:time-millis))
  (to-underlying default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:time-millis))) (default integer))
  (declare (ignore schema)
           (nonnegative-int default))
  (from-underlying default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "int"
                    "logicalType" "time-millis"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:time-millis)))
