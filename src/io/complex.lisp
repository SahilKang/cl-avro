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
   (#:schema #:cl-avro.schema)
   (#:stream #:cl-avro.io.block-stream))
  (:import-from #:cl-avro.io.base
                #:serialize-into
                #:deserialize
                #:serialized-size)
  (:export #:serialize-into
           #:deserialize
           #:serialized-size))
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

(defmethod deserialize ((schema schema:fixed) (stream stream) &key)
  "Read a fixed number of bytes from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((size (schema:size schema))
         (bytes (make-instance schema)))
    (unless (= (read-sequence (schema:raw-buffer bytes) stream) size)
      (error 'end-of-file :stream *error-output*))
    bytes))

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

(defmethod deserialize ((schema schema:union) (stream stream) &key)
  "Read a tagged union from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (let* ((schemas (schema:schemas schema))
         (position (deserialize 'schema:int stream))
         (chosen-schema (elt schemas position))
         (value (deserialize chosen-schema stream)))
    (declare ((simple-array schema:schema (*)) schemas))
    (make-instance schema :object value)))

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

(defmethod deserialize ((schema schema:array) (stream stream) &key)
  "Read an array from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with array-stream = (make-instance 'stream:array-input-stream
                                       :schema (schema:items schema)
                                       :stream stream)
    and vector = (make-instance schema)

    for item = (stream:read-item array-stream)
    until (eq item :eof)
    do (schema:push item vector)

    finally
       (return vector)))

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

(defmethod deserialize ((schema schema:map) (stream stream) &key)
  "Read a map from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with map-stream = (make-instance 'stream:map-input-stream
                                     :schema (schema:values schema)
                                     :stream stream)
    and hash-table = (make-instance schema)

    for pair = (stream:read-item map-stream)
    until (eq pair :eof)
    for (key . value) = pair
    do (setf (schema:hashref key hash-table) value)

    finally
       (return hash-table)))

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

(defmethod deserialize ((schema schema:enum) (stream stream) &key)
  "Read an enum from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  ;; TODO should just have a factory method on the class/schema
  ;; i.e intern enum objects on the class/schema
  (let* ((symbols (schema:symbols schema))
         (position (deserialize 'schema:int stream))
         (chosen-enum (elt symbols position)))
    (declare ((simple-array schema:name (*)) symbols))
    (make-instance schema :enum chosen-enum)))

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

(defmethod deserialize ((schema schema:record) (stream stream) &key)
  "Read a record from STREAM."
  (declare (optimize (speed 3) (safety 0)))
  (loop
    with fields of-type (simple-array schema:field (*)) = (schema:fields schema)
    and initargs of-type list = nil

    for field of-type schema:field across fields
    for value = (deserialize (schema:type field) stream)

    do
       (push value initargs)
       (push (intern (schema:name field) 'keyword) initargs)

    finally
       (return (apply #'make-instance schema initargs))))
