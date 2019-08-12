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

;; all of the primitive schemas are deftypes so this works:
(defmethod validp (schema object)
  ;; if schema is not a type-specifier, then typep will signal simple-error
  (handler-case
      (typep object schema)
    (simple-error ()
      nil)))


;;; null-schema

(defmethod stream-deserialize (stream (schema (eql 'null-schema)))
  "Read as zero bytes."
  nil)

(defmethod stream-serialize (stream (schema (eql 'null-schema)) object)
  "Written as zero bytes."
  nil)

;;; boolean-schema

(defmethod stream-deserialize (stream (schema (eql 'boolean-schema)))
  "Read as a single byte whose value is either 0 (false) or 1 (true)."
  (let ((byte (stream-read-byte stream)))
    (elt '(nil t) byte)))

(defmethod stream-serialize (stream (schema (eql 'boolean-schema)) object)
  "Written as a single byte whose value is either 0 (false) or 1 (true)."
  (if object
      (stream-write-byte stream 1)
      (stream-write-byte stream 0)))

;;; int-schema

(defmethod stream-deserialize (stream (schema (eql 'int-schema)))
  "Read as variable-length zig-zag."
  (read-number stream 32))

(defmethod stream-serialize (stream (schema (eql 'int-schema)) object)
  "Written as variable-length zig-zag."
  (write-number stream object 32))

;;; long-schema

(defmethod stream-deserialize (stream (schema (eql 'long-schema)))
  "Read as variable-length zig-zag."
  (read-number stream 64))

(defmethod stream-serialize (stream (schema (eql 'long-schema)) object)
  "Written as variable-length zig-zag."
  (write-number stream object 64))

;;; float-schema

(defmethod stream-deserialize (stream (schema (eql 'float-schema)))
  "Read as 4 bytes: 32-bit little-endian ieee-754."
  ;; might have to guarantee that this is a byte-stream
  (read-float stream))

(defmethod stream-serialize (stream (schema (eql 'float-schema)) object)
  "Written as 4 bytes: 32-bit little-endian ieee-754."
  (write-float stream object))

;;; double-schema

(defmethod stream-deserialize (stream (schema (eql 'double-schema)))
  "Read as 8 bytes: 64-bit little-endian ieee-754."
  (read-double stream))

(defmethod stream-serialize (stream (schema (eql 'double-schema)) object)
  "Written as 8 bytes: 64-bit little-endian ieee-754."
  (write-double stream object))

;;; bytes-schema

(defmethod stream-deserialize (stream (schema (eql 'bytes-schema)))
  "Read as a long followed by that many bytes."
  (let* ((size (stream-deserialize stream 'long-schema))
         (buf (make-array size :element-type '(unsigned-byte 8))))
    (loop
       for i below size
       for next-byte = (stream-read-byte stream)

       if (eq next-byte :eof)
       do (error "~&Unexpected :eof when deserializing bytes")
       else do (setf (elt buf i) next-byte))
    buf))

(defmethod stream-serialize (stream (schema (eql 'bytes-schema)) (object sequence))
  "Written as a long followed by that many bytes."
  (stream-serialize stream 'long-schema (length object))
  (loop
     for i below (length object)
     do (stream-write-byte stream (elt object i))))

;;; string-schema

(defmethod stream-deserialize (stream (schema (eql 'string-schema)))
  "Read as a long followed by that many utf-8 bytes."
  (let ((bytes (stream-deserialize stream 'bytes-schema)))
    (babel:octets-to-string bytes :encoding :utf-8)))

(defmethod stream-serialize (stream (schema (eql 'string-schema)) (object string))
  "Written as a long followed by that many utf-8 bytes."
  (let ((bytes (babel:string-to-octets object :encoding :utf-8)))
    (stream-serialize stream 'bytes-schema bytes)))


;;; int/long utils

(defun write-zig-zag (n)
  (logxor (ash n 1) (ash n -63)))

(defun read-zig-zag (n)
  (logxor (ash n -1) (- (logand n 1))))

(defun read-variable-length-number (byte-stream bits)
  (let ((value 0))
    (loop
       with max-bytes = (floor bits 7)

       for byte = (stream-read-byte byte-stream)
       for offset from 0

       if (eq byte :eof)
       do (error "~&Reached end of stream before parsing number")

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
       (stream-write-byte byte-stream byte)
       (setf number (ash number -7)))
  (stream-write-byte byte-stream (logand number #xff)))

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
       for next-byte = (stream-read-byte byte-stream)

       if (eq next-byte :eof)
       do (error "~&End of stream reached before parsing float")
       else do (setf (elt buf i) next-byte))
    (ieee-floats:decode-float32 (read-little-endian buf))))

(defun read-double (byte-stream)
  (let ((buf (make-array 8 :element-type '(unsigned-byte 8))))
    (loop
       for i below (length buf)
       for next-byte = (stream-read-byte byte-stream)

       if (eq next-byte :eof)
       do (error "~&End of stream reached before parsing double")
       else do (setf (elt buf i) next-byte))
    (ieee-floats:decode-float64 (read-little-endian buf))))

(defun write-float (byte-stream float)
  (let ((number (ieee-floats:encode-float32 float))
        (buf (make-array 4 :element-type '(unsigned-byte 8))))
    (write-little-endian number buf)
    (loop
       for byte across buf
       do (stream-write-byte byte-stream byte))))

(defun write-double (byte-stream double)
  (let ((number (ieee-floats:encode-float64 double))
        (buf (make-array 8 :element-type '(unsigned-byte 8))))
    (write-little-endian number buf)
    (loop
       for byte across buf
       do (stream-write-byte byte-stream byte))))
