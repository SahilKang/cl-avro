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

(defun number-of-digits (integer)
  (declare (integer integer))
  (if (= 0 integer)
      1
      (ceiling (log (1+ (abs integer)) 10))))

(defun read-big-endian (byte-buf)
  (reduce (let ((offset -1))
            (lambda (byte acc)
              (incf offset)
              (logior acc (ash byte (* offset 8)))))
          byte-buf
          :initial-value 0
          :from-end t))

(defun write-big-endian (number byte-buf)
  (loop
     for offset from (1- (array-dimension byte-buf 0)) downto 0
     for byte = (logand #xff (ash number (* offset -8)))
     do (vector-push byte byte-buf)
     finally (return byte-buf)))

(defun read-twos-complement (byte-buf)
  (let ((value (read-big-endian byte-buf))
        (mask (expt 2 (1- (* 8 (length byte-buf))))))
    (+ (- (logand value mask)) (logand value (lognot mask)))))

(defun write-twos-complement (number byte-buf)
  (let ((twos-complement (if (< number 0)
                             (1+ (lognot (abs number)))
                             number)))
    (write-big-endian twos-complement byte-buf)))

(defmethod validp ((schema decimal-schema) object)
  (and (integerp object)
       (<= (number-of-digits object) (precision schema))))

(defmethod deserialize ((stream stream)
                        (schema decimal-schema)
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (let* ((buf (deserialize stream (underlying-schema schema)))
         (value (read-twos-complement buf)))
    (unless (validp schema value)
      (cerror "Return ~S anyway."
              "~S is too large for precision ~S"
              value
              (precision schema)))
    value))

(defun get-min-buf-length (integer schema)
  (declare (integer integer)
           ((or (eql bytes-schema) fixed-schema) schema))
  (if (eq schema 'bytes-schema)
      (ceiling (/ (1+ (integer-length integer)) 8))
      (size schema)))

(defmethod serialize ((stream stream)
                      (schema decimal-schema)
                      (object integer))
  (let* ((min-length (get-min-buf-length object (underlying-schema schema)))
         (byte-buf (make-array min-length
                               :element-type '(unsigned-byte 8)
                               :fill-pointer 0
                               :adjustable nil)))
    (write-twos-complement object byte-buf)
    (serialize stream (underlying-schema schema) byte-buf)))


(defmethod validp ((schema (eql 'uuid-schema)) object)
  (typep object schema))

(defmethod deserialize ((stream stream)
                        (schema (eql 'uuid-schema))
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (let ((string (deserialize stream 'string-schema)))
    (unless (validp schema string)
      (cerror "Return ~S anyway."
              "~S is not a valid UUID according to RFC-4122"
              string))
    string))

(defmethod serialize ((stream stream)
                      (schema (eql 'uuid-schema))
                      (object string))
  (serialize stream 'string-schema object))


(defmethod validp ((schema (eql 'date-schema)) object)
  (typep object schema))

(defmethod deserialize ((stream stream)
                        (schema (eql 'date-schema))
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (deserialize stream 'int-schema))

(defmethod serialize ((stream stream)
                      (schema (eql 'date-schema))
                      (object integer))
  (serialize stream 'int-schema object))


(defmethod validp ((schema (eql 'time-millis-schema)) object)
  (typep object schema))

(defmethod deserialize ((stream stream)
                        (schema (eql 'time-millis-schema))
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (let ((int (deserialize stream 'int-schema)))
    (unless (validp schema int)
      (cerror "Return ~S anyway."
              "~S is not a valid number of milliseconds after midnight"
              int))
    int))

(defmethod serialize ((stream stream)
                      (schema (eql 'time-millis-schema))
                      (object integer))
  (serialize stream 'int-schema object))


(defmethod validp ((schema (eql 'time-micros-schema)) object)
  (typep object schema))

(defmethod deserialize ((stream stream)
                        (schema (eql 'time-micros-schema))
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (let ((long (deserialize stream 'long-schema)))
    (unless (validp schema long)
      (cerror "Return ~S anyway."
              "~S is not a valid number of microseconds after midnight"
              long))
    long))

(defmethod serialize ((stream stream)
                      (schema (eql 'time-micros-schema))
                      (object integer))
  (serialize stream 'long-schema object))


(defmethod validp ((schema (eql 'timestamp-millis-schema)) object)
  (typep object schema))

(defmethod deserialize ((stream stream)
                        (schema (eql 'timestamp-millis-schema))
                        &optional writer-schema)
  (declare (ignore writer-schema))
  (deserialize stream 'long-schema))

(defmethod serialize ((stream stream)
                      (schema (eql 'timestamp-millis-schema))
                      (object integer))
  (serialize stream 'long-schema object))
