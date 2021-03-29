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
(defpackage #:cl-avro.io.complex
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:serialize-into
                #:serialized-size
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from)
  (:export #:serialize-into
           #:serialized-size
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.io.complex)

;;; fixed schema

(defmethod serialized-size ((object schema:fixed-object))
  (length object))

(defmethod serialize-into
    ((object schema:fixed-object) (vector simple-array) (start fixnum))
  "Write fixed bytes into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let ((raw-buffer (schema:raw-buffer object)))
    (declare ((simple-array (unsigned-byte 8) (*)) raw-buffer))
    (replace vector raw-buffer :start1 start)
    (length raw-buffer)))

;; Read a fixed number of bytes from STREAM.
(define-deserialize-from schema:fixed
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

(defmethod serialize-into
    ((object schema:union-object) (vector simple-array) (start fixnum))
  "Write tagged union into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let* ((position (nth-value 1 (schema:which-one object)))
         (bytes-written (serialize-into position vector start)))
    (declare (fixnum bytes-written))
    (the fixnum
         (+ bytes-written
            (the fixnum
                 (serialize-into
                  (schema:object object) vector (the fixnum (+ start bytes-written))))))))

;; Read a tagged union from STREAM.
(define-deserialize-from schema:union
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

(defmethod serialize-into
    ((object schema:array-object) (vector simple-array) (start fixnum))
  "Write array into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let* ((raw-buffer (schema:raw-buffer object))
         (count (length raw-buffer))
         (bytes-written 0))
    (declare (vector raw-buffer))
    (unless (zerop count)
      (incf (the fixnum bytes-written)
            (the fixnum (serialize-into count vector start)))
      (flet ((serialize-into (elt)
               (incf (the fixnum bytes-written)
                     (the fixnum
                          (serialize-into
                           elt vector (the fixnum (+ start bytes-written)))))))
        (map nil #'serialize-into raw-buffer)))
    (the fixnum
         (+ bytes-written
            (the fixnum
                 (serialize-into 0 vector (the fixnum (+ start bytes-written))))))))

;; Read an array from STREAM.
(define-deserialize-from schema:array
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

(defmethod serialize-into
    ((object schema:map-object) (vector simple-array) (start fixnum))
  "Write map into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let* ((raw-hash-table (schema:raw-hash-table object))
         (count (hash-table-count raw-hash-table))
         (bytes-written 0))
    (unless (zerop count)
      (incf (the fixnum bytes-written)
            (the fixnum (serialize-into count vector start)))
      (flet ((serialize-into (key value)
               (declare (simple-string key))
               (incf (the fixnum bytes-written)
                     (the fixnum
                          (serialize-into
                           key vector (the fixnum (+ start bytes-written)))))
               (incf (the fixnum bytes-written)
                     (the fixnum
                          (serialize-into
                           value vector (the fixnum (+ start bytes-written)))))))
        (maphash #'serialize-into raw-hash-table)))
    (the fixnum
         (+ bytes-written
            (the fixnum
                 (serialize-into 0 vector (the fixnum (+ start bytes-written))))))))

;; Read a map from STREAM.
(define-deserialize-from schema:map
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

(defmethod serialize-into
    ((object schema:enum-object) (vector simple-array) (start fixnum))
  "Write enum into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (let ((position (nth-value 1 (schema:which-one object))))
    (serialize-into position vector start)))

;; Read an enum from STREAM.
(define-deserialize-from schema:enum
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
                    (schema:field object (schema:name field)))))
          (schema:fields (class-of object))
          :initial-value 0))

(defmethod serialize-into
    ((object schema:record-object) (vector simple-array) (start fixnum))
  "Write record into VECTOR."
  (declare (optimize (speed 3) (safety 0))
           ((simple-array (unsigned-byte 8) (*)) vector))
  (loop
    with fields of-type (simple-array schema:field (*)) = (schema:fields
                                                           (class-of object))
    and bytes-written of-type fixnum = 0

    for field of-type schema:field across fields
    for value = (schema:field object (schema:name field))

    do (incf (the fixnum bytes-written)
             (the fixnum
                  (serialize-into
                   value vector (the fixnum (+ start bytes-written)))))

    finally
       (return bytes-written)))

;; Read a record from STREAM.
(define-deserialize-from schema:record
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
