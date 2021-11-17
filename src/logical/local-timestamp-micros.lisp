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
(defpackage #:cl-avro.internal.local-timestamp-micros
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:long #:cl-avro.internal.long))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:cl-avro.internal.logical.datetime
                #:unix-epoch
                #:local-timezone
                #:local-date
                #:local-hour-minute
                #:local-second-micros
                #:adjust-for-daylight-savings
                #:local-unix-epoch-diff)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:cl-avro.internal.local-timestamp-micros)

;;; local-timestamp-micros

(defclass api:local-timestamp-micros
    (api:logical-object local-date local-hour-minute local-second-micros)
  ()
  (:metaclass api:logical-schema)
  (:documentation
   "Avro local-timestamp-micros schema.

This represents a microsecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:local-timestamp-micros))))
  (declare (ignore schema))
  'api:long)

(defmethod initialize-instance :after
    ((instance api:local-timestamp-micros)
     &key year month day hour minute microsecond)
  (when (or year month hour minute microsecond)
    (multiple-value-bind (second remainder)
        (truncate microsecond (* 1000 1000))
      (local-time:encode-timestamp
       (* remainder 1000) second minute hour day month year
       :into instance :timezone (local-timezone instance)))))

;;; to/from-underlying

(deftype second-diff ()
  (let ((min (truncate (- (expt 2 63)) (* 1000 1000)))
        (max (truncate (1- (expt 2 63)) (* 1000 1000))))
    `(integer ,min ,max)))

(declaim
 (ftype (function (api:local-timestamp-micros) (values api:long &optional))
        to-underlying))
(defun to-underlying (local-timestamp-micros)
  "Serialized as the number of microseconds from 1970-01-01T00:00:00.000000."
  (let ((diff (local-unix-epoch-diff local-timestamp-micros)))
    (multiple-value-bind (second-diff diff)
        (local-time-duration:duration-as diff :sec)
      (declare (second-diff second-diff))
      (let ((nanosecond-diff (local-time-duration:duration-as diff :nsec)))
        (declare ((mod 1000000000) nanosecond-diff))
        (+ (* second-diff 1000 1000)
           (truncate nanosecond-diff 1000))))))

(declaim
 (ftype (function (api:long) (values api:local-timestamp-micros &optional))
        from-underlying))
(defun from-underlying (microseconds-from-local-unix-epoch)
  (multiple-value-bind
        (seconds-from-local-unix-epoch microseconds-from-local-unix-epoch)
      (truncate microseconds-from-local-unix-epoch (* 1000 1000))
    (let ((timestamp
            (local-time:adjust-timestamp!
                (unix-epoch local-time:*default-timezone*)
              (offset :sec seconds-from-local-unix-epoch)
              (offset :nsec (* microseconds-from-local-unix-epoch 1000)))))
      (adjust-for-daylight-savings timestamp)
      (change-class timestamp 'api:local-timestamp-micros))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:local-timestamp-micros))))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object api:local-timestamp-micros))
  (long:serialized-size (to-underlying object)))

;;; serialize

(defmethod internal:serialize
    ((object api:local-timestamp-micros) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (long:serialize-into-vector (to-underlying object) into start))

(defmethod internal:serialize
    ((object api:local-timestamp-micros) (into stream) &key)
  (long:serialize-into-stream (to-underlying object) into))

(defmethod api:serialize
    ((object api:local-timestamp-micros)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object)) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema (eql (find-class 'api:local-timestamp-micros)))
     (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (microseconds-from-local-unix-epoch bytes-read)
      (api:deserialize 'api:long input :start start)
    (values (from-underlying microseconds-from-local-unix-epoch) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:local-timestamp-micros)))
     (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (microseconds-from-local-unix-epoch bytes-read)
      (api:deserialize 'api:long input)
    (values (from-underlying microseconds-from-local-unix-epoch) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:local-timestamp-micros))) (input vector)
     &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:long input start))

(defmethod internal:skip
    ((schema (eql (find-class 'api:local-timestamp-micros))) (input stream)
     &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:long input))

(defmethod api:compare
    ((schema (eql (find-class 'api:local-timestamp-micros)))
     (left vector) (right vector) &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:long left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:local-timestamp-micros)))
     (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:long left right))

;;; coerce

(defmethod api:coerce
    ((object api:local-timestamp-micros)
     (schema (eql (find-class 'api:local-timestamp-micros))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:local-timestamp-micros) (schema (eql 'api:long)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:local-timestamp-micros) (schema (eql 'api:float)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:float))

(defmethod api:coerce
    ((object api:local-timestamp-micros) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql (find-class 'api:local-timestamp-micros))))
  (declare (ignore schema)
           (api:long object))
  (from-underlying object))

(defmethod api:coerce
    ((object api:local-timestamp-millis)
     (schema (eql (find-class 'api:local-timestamp-micros))))
  (declare (ignore schema))
  (change-class object 'api:local-timestamp-micros))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:local-timestamp-micros))
  (to-underlying default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:local-timestamp-micros))) (default integer))
  (declare (ignore schema)
           (api:long default))
  (from-underlying default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "long"
                    "logicalType" "local-timestamp-micros"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:local-timestamp-micros)))
