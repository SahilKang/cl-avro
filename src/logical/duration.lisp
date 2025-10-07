;;; Copyright 2021, 2024 Google LLC
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
(defpackage #:cl-avro.internal.duration
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:mop #:cl-avro.internal.mop)
   (#:little-endian #:cl-avro.internal.little-endian))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:uint32
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:cl-avro.internal.compare
                #:compare-byte-vectors
                #:compare-byte-streams)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:cl-avro.internal.duration)

;;; duration

(defclass api:duration (api:logical-schema)
  ((underlying
    :initarg :underlying
    :reader internal:underlying
    :late-type api:fixed))
  (:metaclass mop:schema-class)
  (:scalars :underlying)
  (:object-class api:duration-object)
  (:default-initargs
   :underlying (error "Must supply UNDERLYING"))
  (:documentation
   "Metaclass of avro duration schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:duration) (superclass api:logical-schema))
  t)

(defmethod mop:early->late
    ((class api:duration) (name (eql 'underlying)) type value)
  (let* ((value (call-next-method))
         (size (api:size value)))
    (assert (= size 12) () "Size of fixed schema must be 12, not ~S" size)
    value))

;;; duration-object

(defclass api:duration-object (time-interval:time-interval api:logical-object)
  ((time-interval::months
    :type uint32
    :documentation "Number of months.")
   (time-interval::days
    :type uint32
    :documentation "Number of days.")
   (milliseconds
    :type uint32
    :accessor milliseconds
    :documentation "Number of milliseconds."))
  (:documentation
   "Base class for instances of an avro duration schema."))

(declaim (boolean *normalize-p*))
(defparameter *normalize-p* t)

(declaim (ftype (function (api:duration-object) (values &optional)) normalize))
(defun normalize (duration-object)
  (let ((*normalize-p* nil))
    (with-accessors
          ((years time-interval::interval-years)
           (months time-interval::interval-months)
           (weeks time-interval::interval-weeks)
           (days time-interval::interval-days)
           (hours time-interval::interval-hours)
           (minutes time-interval::interval-minutes)
           (seconds time-interval::interval-seconds)
           (milliseconds milliseconds)
           (nanoseconds time-interval::interval-nanoseconds))
        duration-object
      (declare (uint32 months days milliseconds)
               (integer years weeks hours minutes seconds nanoseconds))
      (incf months (* years 12))
      (setf years 0)

      (incf days (* weeks 7))
      (setf weeks 0)

      (incf minutes (* 60 hours))
      (incf seconds (* 60 minutes))
      (setf hours 0
            minutes 0)

      (setf milliseconds (+ (* 1000 seconds)
                            (truncate nanoseconds (* 1000 1000))))))
  (values))

(defmethod (setf closer-mop:slot-value-using-class)
    (new-value (class api:duration) (object api:duration-object) slot)
  (prog1 (call-next-method)
    (when *normalize-p*
      (normalize object))))

(defmethod initialize-instance :around
    ((instance api:duration-object) &key)
  (let ((*normalize-p* nil))
    (call-next-method)))

(defmethod initialize-instance :after
    ((instance api:duration-object) &key (milliseconds 0))
  (multiple-value-bind (seconds remainder)
      (truncate milliseconds 1000)
    (incf (time-interval::interval-seconds instance) seconds)
    (incf (time-interval::interval-nanoseconds instance)
          (* remainder 1000 1000)))
  (normalize instance))

(defmethod api:months
    ((object api:duration-object))
  "Return months for OBJECT."
  (time-interval::interval-months object))

(defmethod api:days
    ((object api:duration-object))
  "Return days for OBJECT."
  (time-interval::interval-days object))

(defmethod api:milliseconds
    ((object api:duration-object))
  "Return milliseconds for OBJECT."
  (milliseconds object))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:duration))
  (declare (ignore schema))
  12)

(defmethod api:serialized-size
    ((object api:duration-object))
  (declare (ignore object))
  12)

;;; serialize

(defmethod internal:serialize
    ((object api:duration-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (with-accessors
        ((months time-interval::interval-months)
         (days time-interval::interval-days)
         (milliseconds milliseconds))
      object
    (little-endian:uint32->vector months into start)
    (little-endian:uint32->vector days into (+ start 4))
    (little-endian:uint32->vector milliseconds into (+ start 8)))
  12)

(defmethod internal:serialize
    ((object api:duration-object) (into stream) &key)
  (with-accessors
        ((months time-interval::interval-months)
         (days time-interval::interval-days)
         (milliseconds milliseconds))
      object
    (little-endian:uint32->stream months into)
    (little-endian:uint32->stream days into)
    (little-endian:uint32->stream milliseconds into))
  12)

(defmethod api:serialize
    ((object api:duration-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 22 12) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema api:duration) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (months days milliseconds)
      (values (little-endian:vector->uint32 input start)
              (little-endian:vector->uint32 input (+ start 4))
              (little-endian:vector->uint32 input (+ start 8)))
    (values (make-instance
             schema :months months :days days :milliseconds milliseconds)
            12)))

(defmethod api:deserialize
    ((schema api:duration) (input stream) &key)
  (multiple-value-bind (months days milliseconds)
      (values (little-endian:stream->uint32 input)
              (little-endian:stream->uint32 input)
              (little-endian:stream->uint32 input))
    (values (make-instance
             schema :months months :days days :milliseconds milliseconds)
            12)))

;;; compare

(defmethod internal:skip
    ((schema api:duration) (input vector) &optional start)
  (declare (ignore schema input start))
  12)

(defmethod internal:skip
    ((schema api:duration) (input stream) &optional start)
  (declare (ignore schema start))
  (loop repeat 12 do (read-byte input))
  12)

(defmethod api:compare
    ((schema api:duration) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (let ((left-end (+ left-start 12))
        (right-end (+ right-start 12)))
    (compare-byte-vectors
     left right left-start right-start left-end right-end)))

(defmethod api:compare
    ((schema api:duration) (left stream) (right stream) &key)
  (compare-byte-streams left right 12 12))

;;; coerce

(defmethod api:coerce
    ((object api:duration-object) (schema api:duration))
  (change-class object schema))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:duration-object))
  (let ((buffer (make-array 12 :element-type 'uint8)))
    (with-accessors
          ((months time-interval::interval-months)
           (days time-interval::interval-days)
           (milliseconds milliseconds))
        default
      (little-endian:uint32->vector months buffer 0)
      (little-endian:uint32->vector days buffer 4)
      (little-endian:uint32->vector milliseconds buffer 8))
    (babel:octets-to-string buffer :encoding :latin-1)))

(defmethod internal:deserialize-field-default
    ((schema api:duration) (default string))
  (multiple-value-bind (months days milliseconds)
      (let ((buffer (babel:string-to-octets default :encoding :latin-1)))
        ;; assuming valid input like this size 12 is unsafe
        (declare ((simple-array uint8 (12)) buffer))
        (values (little-endian:vector->uint32 buffer 0)
                (little-endian:vector->uint32 buffer 4)
                (little-endian:vector->uint32 buffer 8)))
    (make-instance
     schema :months months :days days :milliseconds milliseconds)))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" nil
                    "logicalType" "duration"))
              fullname->schema
              enclosing-namespace)
      (let ((underlying (internal:read-jso (st-json:getjso "type" jso)
                                           fullname->schema
                                           enclosing-namespace)))
        (handler-case
            (make-instance 'api:duration :underlying underlying)
          (error ()
            underlying)))))

(defmethod internal:logical-name
    ((schema api:duration))
  (declare (ignore schema))
  "duration")
