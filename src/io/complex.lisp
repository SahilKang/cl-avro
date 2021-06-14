;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.io.complex
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:serialized-size
                #:serialize-into-vector
                #:serialize-into-stream
                #:define-serialize-into
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from)
  (:export #:serialize-into-vector
           #:serialize-into-stream
           #:serialized-size
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.io.complex)

;;; fixed schema

(defmethod serialized-size ((object schema:fixed-object))
  (length object))

(define-serialize-into schema:fixed-object
  "Write fixed bytes."
  `(let ((raw-buffer (schema:raw-buffer object)))
     (declare ((simple-array (unsigned-byte 8) (*)) raw-buffer))
     ,(if vectorp
          '(replace vector raw-buffer :start1 start)
          '(write-sequence raw-buffer stream))
     (length raw-buffer)))

(define-deserialize-from schema:fixed
  "Read a fixed number of bytes."
  `(let ((size (schema:size schema))
         (bytes (make-instance schema)))
     ,@(when vectorp
         `((unless (>= (- (length vector) start) size)
             (error "Not enough bytes"))))
     ,(if vectorp
          `(replace (schema:raw-buffer bytes) vector
                    :start2 start :end2 (+ start size))
          `(unless (= (read-sequence (schema:raw-buffer bytes) stream) size)
             (error 'end-of-file :stream *error-output*)))
     (values bytes size)))

;;; union schema

(defmethod serialized-size ((object schema:union-object))
  (let ((position (nth-value 1 (schema:which-one object))))
    (+ (serialized-size position)
       (serialized-size (schema:object object)))))

(define-serialize-into schema:union-object
  "Write tagged union."
  `(let* ((position (nth-value 1 (schema:which-one object)))
          (bytes-written
            ,(if vectorp
                 '(serialize-into-vector position vector start)
                 '(serialize-into-stream position stream)))
          (object (schema:object object)))
     (declare (fixnum bytes-written))
     (+ bytes-written
        ,(if vectorp
             '(serialize-into-vector object vector (+ start bytes-written))
             '(serialize-into-stream object stream)))))

(define-deserialize-from schema:union
  "Read a tagged union."
  `(let ((schemas (schema:schemas schema)))
     (declare ((simple-array schema:schema (*)) schemas))
     (multiple-value-bind (position bytes-read)
         ,(if vectorp
              `(deserialize-from-vector 'schema:int vector start)
              `(deserialize-from-stream 'schema:int stream))
       (let ((chosen-schema (elt schemas position)))
         (multiple-value-bind (value more-bytes-read)
             ,(if vectorp
                  `(deserialize-from-vector chosen-schema vector (+ start bytes-read))
                  `(deserialize-from-stream chosen-schema stream))
           (values
            (make-instance schema :object value)
            (+ bytes-read more-bytes-read)))))))

;;; array schema

(defmethod serialized-size ((object schema:array-object))
  (let ((count (length object)))
    (if (zerop count)
        1
        (1+ (reduce (lambda (agg x)
                      (+ agg (serialized-size x)))
                    object
                    :initial-value (serialized-size count))))))

(define-serialize-into schema:array-object
  "Write array."
  `(let* ((raw-buffer (schema:raw-buffer object))
          (count (length raw-buffer))
          (bytes-written 0))
     (declare (vector raw-buffer))
     (unless (zerop count)
       (incf bytes-written
             ,(if vectorp
                  '(serialize-into-vector count vector start)
                  '(serialize-into-stream count stream)))
       (flet ((serialize-into (elt)
                (incf bytes-written
                      ,(if vectorp
                           '(serialize-into-vector
                             elt vector (+ start bytes-written))
                           '(serialize-into-stream elt stream)))))
         (map nil #'serialize-into raw-buffer)))
     (+ bytes-written
        ,(if vectorp
             '(serialize-into-vector 0 vector (+ start bytes-written))
             '(serialize-into-stream 0 stream)))))

(define-deserialize-from schema:array
  "Read an array."
  `(loop
     with total-bytes-read = 0
     and item-schema = (schema:items schema)
     and output = (make-instance schema)

     for (count bytes-read)
       = (multiple-value-list
          ,(if vectorp
               `(deserialize-from-vector 'schema:long vector (+ start total-bytes-read))
               `(deserialize-from-stream 'schema:long stream)))
     do (incf total-bytes-read bytes-read)
     until (zerop count)

     when (minusp count) do
       (setf count (abs count))
       (incf total-bytes-read
             (nth-value 1 ,(if vectorp
                               `(deserialize-from-vector
                                 'schema:long vector (+ start total-bytes-read))
                               `(deserialize-from-stream 'schema:long stream))))

     do (loop
          repeat count
          for (item bytes-read)
            = (multiple-value-list
               ,(if vectorp
                    `(deserialize-from-vector item-schema vector (+ start total-bytes-read))
                    `(deserialize-from-stream item-schema stream)))
          do
             (incf total-bytes-read bytes-read)
             (schema:push item output))

     finally
        (return (values output total-bytes-read))))

;;; map schema

(defmethod serialized-size ((object schema:map-object))
  (let ((count (schema:generic-hash-table-count object))
        (result 1))
    (unless (zerop count)
      (incf result (serialized-size count))
      (flet ((incf-result (key value)
               (incf result (serialized-size key))
               (incf result (serialized-size value))))
        (schema:hashmap #'incf-result object)))
    result))

(define-serialize-into schema:map-object
  "Write map."
  `(let* ((raw-hash-table (schema:raw-hash-table object))
          (count (hash-table-count raw-hash-table))
          (bytes-written 0))
     (unless (zerop count)
       (incf bytes-written
             ,(if vectorp
                  '(serialize-into-vector count vector start)
                  '(serialize-into-stream count stream)))
       (flet ((serialize-into (key value)
                (declare (simple-string key))
                (incf bytes-written
                      ,(if vectorp
                           '(serialize-into-vector
                             key vector (+ start bytes-written))
                           '(serialize-into-stream key stream)))
                (incf bytes-written
                      ,(if vectorp
                           '(serialize-into-vector
                             value vector (+ start bytes-written))
                           '(serialize-into-stream value stream)))))
         (maphash #'serialize-into raw-hash-table)))
     (+ bytes-written
        ,(if vectorp
             '(serialize-into-vector 0 vector (+ start bytes-written))
             '(serialize-into-stream 0 stream)))))

(define-deserialize-from schema:map
  "Read a map."
  `(loop
     with total-bytes-read = 0
     and value-schema = (schema:values schema)
     and output = (make-instance schema)

     for (count bytes-read)
       = (multiple-value-list
          ,(if vectorp
               `(deserialize-from-vector 'schema:long vector (+ start total-bytes-read))
               `(deserialize-from-stream 'schema:long stream)))
     do (incf total-bytes-read bytes-read)
     until (zerop count)

     when (minusp count) do
       (setf count (abs count))
       (incf total-bytes-read
             (nth-value 1 ,(if vectorp
                               `(deserialize-from-vector
                                 'schema:long vector (+ start total-bytes-read))
                               `(deserialize-from-stream 'schema:long stream))))

     do (loop
          repeat count
          for (key bytes-read)
            = (multiple-value-list
               ,(if vectorp
                    `(deserialize-from-vector
                      'schema:string vector (+ start total-bytes-read))
                    `(deserialize-from-stream 'schema:string stream)))
          for (value more-bytes-read)
            = (multiple-value-list
               ,(if vectorp
                    `(deserialize-from-vector
                      value-schema vector (+ start total-bytes-read bytes-read))
                    `(deserialize-from-stream value-schema stream)))
          do
             (incf total-bytes-read (+ bytes-read more-bytes-read))
             (setf (schema:hashref key output) value))

     finally
        (return (values output total-bytes-read))))

;;; enum schema

(defmethod serialized-size ((object schema:enum-object))
  (let ((position (nth-value 1 (schema:which-one object))))
    (serialized-size position)))

(define-serialize-into schema:enum-object
  "Write enum."
  `(let ((position (nth-value 1 (schema:which-one object))))
     ,(if vectorp
          '(serialize-into-vector position vector start)
          '(serialize-into-stream position stream))))

(define-deserialize-from schema:enum
  "Read an enum."
  `(multiple-value-bind (position bytes-read)
       ,(if vectorp
            `(deserialize-from-vector 'schema:int vector start)
            `(deserialize-from-stream 'schema:int stream))
     (let* ((symbols (schema:symbols schema))
            (chosen-enum (elt symbols position)))
       (declare ((simple-array schema:name (*)) symbols))
       (values (make-instance schema :enum chosen-enum) bytes-read))))

;;; record schema

(defmethod serialized-size ((object schema:record-object))
  (reduce (lambda (agg field)
            (+ agg (serialized-size
                    (slot-value object (nth-value 1 (schema:name field))))))
          (schema:fields (class-of object))
          :initial-value 0))

(define-serialize-into schema:record-object
  "Write record."
  `(loop
     with fields of-type (simple-array schema:field (*)) = (schema:fields
                                                            (class-of object))
     and bytes-written of-type fixnum = 0

     for field of-type schema:field across fields
     for value = (slot-value object (nth-value 1 (schema:name field)))

     do (incf bytes-written
              ,(if vectorp
                   '(serialize-into-vector
                     value vector (+ start bytes-written))
                   '(serialize-into-stream value stream)))

     finally
        (return bytes-written)))

(define-deserialize-from schema:record
  "Read a record."
  `(loop
     with fields of-type (simple-array schema:field (*)) = (schema:fields schema)
     and initargs of-type list = nil
     and total-bytes-read = 0

     for field of-type schema:field across fields
     for (value bytes-read)
       = (multiple-value-list
          ,(if vectorp
               `(deserialize-from-vector
                 (schema:type field) vector (+ start total-bytes-read))
               `(deserialize-from-stream (schema:type field) stream)))

     do
        (push value initargs)
        (push (intern (schema:name field) 'keyword) initargs)
        (incf total-bytes-read bytes-read)

     finally
        (return (values (apply #'make-instance schema initargs) total-bytes-read))))
