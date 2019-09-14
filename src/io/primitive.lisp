;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defgeneric validp (schema object)
  (:documentation
   "Determine if OBJECT is valid according to avro SCHEMA."))

(defgeneric deserialize (stream-or-seq schema)
  (:documentation
   "Deserialize next object from STREAM-OR-SEQ according to avro SCHEMA or :EOF."))

(defgeneric serialize (output-stream schema object)
  (:documentation
   "Serialize OBJECT into OUTPUT-STREAM according to avro SCHEMA.

If OUTPUT-STREAM is nil, then the serialized bytes are returned as a vector."))


;; specialize validp methods for primitive avro types:
(macrolet
    ((make-validp-methods (&rest symbols)
       (loop
          for s in symbols
          for externalp = (eq :external (nth-value 1 (find-symbol (string s))))
          unless externalp
          do (error "~&~S is not an external symbol" s))
       `(progn
          ,@(loop
               for s in symbols
               collect `(defmethod validp ((schema (eql ',s)) object)
                          (typep object schema))))))
  (make-validp-methods
   null-schema
   boolean-schema
   int-schema
   long-schema
   float-schema
   double-schema
   bytes-schema
   string-schema))

(defmethod serialize :before (output-stream schema object)
  (unless (validp schema object)
    (error "~&Object ~A does not match schema ~A" object schema)))


(defclass input-stream (fundamental-binary-input-stream)
  ((bytes
    :initform (error "Must supply :bytes")
    :initarg :bytes
    :type (typed-vector (unsigned-byte 8)))
   (position
    :initform 0
    :type (integer 0)))
  (:documentation
   "A binary input stream backed by a vector of bytes."))

(defmethod stream-read-byte ((stream input-stream))
  (with-slots (bytes position) stream
    (if (= position (length bytes))
        :eof
        (prog1 (elt bytes position)
          (incf position)))))

(defclass output-stream (fundamental-binary-output-stream)
  ((bytes
    :initform (make-array 0
                          :element-type '(unsigned-byte 8)
                          :adjustable t
                          :fill-pointer 0)
    :initarg :bytes
    :type (typed-vector (unsigned-byte 8))
    :reader bytes))
  (:documentation
   "A binary output stream backed by a vector of bytes."))

(defmethod stream-write-byte ((stream output-stream) (byte integer))
  (check-type byte (unsigned-byte 8))
  (with-slots (bytes) stream
    (vector-push-extend byte bytes))
  byte)


(defmethod deserialize ((bytes sequence) schema)
  (let ((input-stream (make-instance 'input-stream :bytes (coerce bytes 'vector))))
    (deserialize input-stream schema)))

(defmethod serialize ((nada null) schema object)
  "Return a vector of bytes instead of serializing to a stream."
  (declare (ignore nada))
  (let ((output-stream (make-instance 'output-stream)))
    (serialize output-stream schema object)
    (bytes output-stream)))


;;; null-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'null-schema)))
  "Read as zero bytes."
  nil)

(defmethod serialize ((stream stream)
                      (schema (eql 'null-schema))
                      object)
  "Written as zero bytes."
  nil)

;;; boolean-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'boolean-schema)))
  "Read as a single byte whose value is either 0 (false) or 1 (true)."
  (let ((byte (read-byte stream nil :eof)))
    (elt '(nil t) byte)))

(defmethod serialize ((stream stream)
                      (schema (eql 'boolean-schema))
                      object)
  "Written as a single byte whose value is either 0 (false) or 1 (true)."
  (write-byte (if object 1 0) stream))

;;; int-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'int-schema)))
  "Read as variable-length zig-zag."
  (read-number stream 32))

(defmethod serialize ((stream stream)
                      (schema (eql 'int-schema))
                      object)
  "Written as variable-length zig-zag."
  (write-number stream object 32))

;;; long-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'long-schema)))
  "Read as variable-length zig-zag."
  (read-number stream 64))

(defmethod serialize ((stream stream)
                      (schema (eql 'long-schema))
                      object)
  "Written as variable-length zig-zag."
  (write-number stream object 64))

;;; float-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'float-schema)))
  "Read as 4 bytes: 32-bit little-endian ieee-754."
  ;; might have to guarantee that this is a byte-stream
  (read-float stream))

(defmethod serialize ((stream stream)
                      (schema (eql 'float-schema))
                      object)
  "Written as 4 bytes: 32-bit little-endian ieee-754."
  (write-float stream object))

;;; double-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'double-schema)))
  "Read as 8 bytes: 64-bit little-endian ieee-754."
  (read-double stream))

(defmethod serialize ((stream stream)
                      (schema (eql 'double-schema))
                      object)
  "Written as 8 bytes: 64-bit little-endian ieee-754."
  (write-double stream object))

;;; bytes-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'bytes-schema)))
  "Read as a long followed by that many bytes."
  (let* ((size (deserialize stream 'long-schema))
         (buf (make-array size :element-type '(unsigned-byte 8))))
    (loop
       for i below size
       for next-byte = (read-byte stream nil :eof)

       if (eq next-byte :eof)
       do (error 'end-of-file :stream *error-output*)
       else do (setf (elt buf i) next-byte))
    buf))

(defmethod serialize ((stream stream)
                      (schema (eql 'bytes-schema))
                      (object sequence))
  "Written as a long followed by that many bytes."
  (serialize stream 'long-schema (length object))
  (loop
     for i below (length object)
     do (write-byte (elt object i) stream)))

;;; string-schema

(defmethod deserialize ((stream stream)
                        (schema (eql 'string-schema)))
  "Read as a long followed by that many utf-8 bytes."
  (let ((bytes (deserialize stream 'bytes-schema)))
    (babel:octets-to-string bytes :encoding :utf-8)))

(defmethod serialize ((stream stream)
                      (schema (eql 'string-schema))
                      (object string))
  "Written as a long followed by that many utf-8 bytes."
  (let ((bytes (babel:string-to-octets object :encoding :utf-8)))
    (serialize stream 'bytes-schema bytes)))


;;; int/long utils

(defun write-zig-zag (n)
  (logxor (ash n 1) (ash n -63)))

(defun read-zig-zag (n)
  (logxor (ash n -1) (- (logand n 1))))

(defun read-variable-length-number (byte-stream bits)
  (let ((value 0))
    (loop
       with max-bytes = (floor bits 7)

       for byte = (read-byte byte-stream nil :eof)
       for offset from 0

       if (eq byte :eof)
       do (error 'end-of-file :stream *error-output*)

       else if (> offset max-bytes)
       do (error "~&Too many bytes for number, expected: ~A bytes max" max-bytes)

       else do (setf value (logior value (ash (logand byte #x7f)
                                              (* 7 offset))))

       until (zerop (logand byte #x80)))
    value))

(defun write-variable-length-number (byte-stream number)
  (loop
     until (zerop (logand number (lognot #x7f)))
     for byte = (logior (logand number #x7f) #x80)
     do
       (write-byte byte byte-stream)
       (setf number (ash number -7)))
  (write-byte (logand number #xff) byte-stream))

(defun get-range (bits)
  (let* ((max (1- (expt 2 (1- bits))))
         (min (- (1+ max))))
    (list min max)))

(defun assert-range (number bits)
  (destructuring-bind (min max) (get-range bits)
    (when (or (> number max) (< number min))
      (error "~&Number ~A out-of-range for ~A-bit values" number bits))))

(defun read-number (byte-stream bits)
  (let ((number (read-zig-zag (read-variable-length-number byte-stream bits))))
    (assert-range number bits)
    number))

(defun write-number (byte-stream number bits)
  (assert-range number bits)
  (write-variable-length-number byte-stream (write-zig-zag number)))


;;; float/double utils

(defun read-little-endian (byte-buf)
  (reduce (let ((offset -1))
            (lambda (acc byte)
              (incf offset)
              (logior acc (ash byte (* offset 8)))))
          byte-buf
          :initial-value 0))

(defun write-little-endian (number byte-buf)
  (loop
     for offset below (length byte-buf)
     for byte = (logand #xff (ash number (* offset -8)))
     do (setf (elt byte-buf offset) byte))
  byte-buf)

(defun read-float (byte-stream)
  (let ((buf (make-array 4 :element-type '(unsigned-byte 8))))
    (loop
       for i below (length buf)
       for next-byte = (read-byte byte-stream nil :eof)

       if (eq next-byte :eof)
       do (error 'end-of-file :stream *error-output*)
       else do (setf (elt buf i) next-byte))
    (ieee-floats:decode-float32 (read-little-endian buf))))

(defun read-double (byte-stream)
  (let ((buf (make-array 8 :element-type '(unsigned-byte 8))))
    (loop
       for i below (length buf)
       for next-byte = (read-byte byte-stream nil :eof)

       if (eq next-byte :eof)
       do (error 'end-of-file :stream *error-output*)
       else do (setf (elt buf i) next-byte))
    (ieee-floats:decode-float64 (read-little-endian buf))))

(defun write-float (byte-stream float)
  (let ((number (ieee-floats:encode-float32 float))
        (buf (make-array 4 :element-type '(unsigned-byte 8))))
    (write-little-endian number buf)
    (loop
       for byte across buf
       do (write-byte byte byte-stream))))

(defun write-double (byte-stream double)
  (let ((number (ieee-floats:encode-float64 double))
        (buf (make-array 8 :element-type '(unsigned-byte 8))))
    (write-little-endian number buf)
    (loop
       for byte across buf
       do (write-byte byte byte-stream))))
