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

(defgeneric compare (schema left right)
  (:documentation
   "Return 0, -1, or 1 if LEFT is equal to, less than, or greater than RIGHT.

LEFT and RIGHT should be avro serialized data."))

(defmethod compare (schema (left sequence) right)
  (let ((left (make-instance 'input-stream :bytes (coerce left 'vector))))
    (compare schema left right)))

(defmethod compare (schema left (right sequence))
  (let ((right (make-instance 'input-stream :bytes (coerce right 'vector))))
    (compare schema left right)))


(defun compare-boolean (left-stream right-stream)
  (- (read-byte left-stream) (read-byte right-stream)))

(defun compare-number (left-number right-number)
  (cond
    ((= left-number right-number) 0)
    ((< left-number right-number) -1)
    (t 1)))

(defun compare-int (left-stream right-stream)
  (compare-number (read-number left-stream 32) (read-number right-stream 32)))

(defun compare-long (left-stream right-stream)
  (compare-number (read-number left-stream 64) (read-number right-stream 64)))

(defun compare-float (left-stream right-stream)
  (compare-number (read-float left-stream) (read-float right-stream)))

(defun compare-double (left-stream right-stream)
  (compare-number (read-double left-stream) (read-double right-stream)))

(defun read-buf (stream num-bytes)
  (let* ((buf (make-array num-bytes :element-type '(unsigned-byte 8)))
         (bytes-read (read-sequence buf stream)))
    (unless (= bytes-read num-bytes)
      (error "Read ~A bytes instead of expected ~A" bytes-read num-bytes))
    buf))

(defun compare-buf (left-buf right-buf)
  (loop
     for x across left-buf
     for y across right-buf

     if (< x y)
     return -1
     else if (> x y)
     return 1

     finally (return (compare-number (length left-buf) (length right-buf)))))

(defun compare-bytes (left-stream right-stream)
  (let ((left-buf (read-buf left-stream (read-number left-stream 64)))
        (right-buf (read-buf right-stream (read-number right-stream 64))))
    (compare-buf left-buf right-buf)))

(defun compare-string (left-stream right-stream)
  (compare-bytes left-stream right-stream))

(defun compare-fixed (fixed-schema left-stream right-stream)
  (let* ((size (size fixed-schema))
         (left-buf (read-buf left-stream size))
         (right-buf (read-buf right-stream size)))
    (compare-buf left-buf right-buf)))

(defun compare-array (array-schema left-stream right-stream)
  (loop
     with item-schema = (item-schema array-schema)
     with left-array-stream = (make-instance 'array-input-stream
                                             :input-stream left-stream
                                             :schema item-schema)
     with right-array-stream = (make-instance 'array-input-stream
                                              :input-stream right-stream
                                              :schema item-schema)

     for x = (stream-read-item left-array-stream)
     for y = (stream-read-item right-array-stream)
     until (or (eq x :eof) (eq y :eof))

     ;; TODO get rid of this extra serializing
     for compare = (compare item-schema
                            (serialize nil item-schema x)
                            (serialize nil item-schema y))
     unless (zerop compare)
     return compare

     finally (return
               (cond
                 ((and (eq x :eof) (eq y :eof)) 0)
                 ((eq x :eof) -1)
                 (t 1)))))

(defun compare-enum (left-stream right-stream)
  (compare-int left-stream right-stream))

(defun compare-union (union-schema left-stream right-stream)
  (let* ((left-pos (read-number left-stream 64))
         (right-pos (read-number right-stream 64))
         (compare (compare-number left-pos right-pos)))
    (if (not (zerop compare))
        compare
        (compare (elt (schemas union-schema) left-pos)
                 left-stream
                 right-stream))))

(defun consume-maps (map-schema left-stream right-stream)
  (let ((value-schema (value-schema map-schema)))
    (loop
       with map-stream = (make-instance 'map-input-stream
                                        :input-stream left-stream
                                        :schema value-schema)
       for pair = (stream-read-item map-stream)
       until (eq pair :eof))
    (loop
       with map-stream = (make-instance 'map-input-stream
                                        :input-stream right-stream
                                        :schema value-schema)
       for pair = (stream-read-item map-stream)
       until (eq pair :eof))))

(defun compare-record (record-schema left-stream right-stream)
  (loop
     for field-schema across (field-schemas record-schema)

     for order = (order field-schema)
     for field-type = (field-type field-schema)

     if (string= order "ignore")
     do (if (typep field-type 'map-schema)
            (consume-maps field-type left-stream right-stream)
            (compare field-type left-stream right-stream))
     else do (let ((compare (compare field-type left-stream right-stream)))
               (unless (zerop compare)
                 (return-from compare-record
                   (if (string= "ascending" order)
                       compare
                       (* -1 compare)))))

     finally (return 0)))


(defmethod compare ((schema (eql 'null-schema))
                    (left stream)
                    (right stream))
  0)

(macrolet
    ((def-compare-methods (&rest symbols)
       (let* ((schema-function-pairs
               (mapcar (lambda (symbol)
                         (let ((compare (find-symbol (format nil "COMPARE-~A" symbol))))
                           (unless compare
                             (error "Could not find compare function for ~S" symbol))
                           (multiple-value-bind (schema schemap)
                               (find-symbol (format nil "~A-SCHEMA" symbol))
                             (unless (eq schemap :external)
                               (error "Could not find schema for ~S" symbol))
                             (cons schema compare))))
                       symbols))
              (defmethods
               (mapcar (lambda (pair)
                         `(defmethod compare ((schema (eql ',(car pair)))
                                              (left stream)
                                              (right stream))
                            (,(cdr pair) left right)))
                       schema-function-pairs)))
         `(progn
            ,@defmethods))))
  (def-compare-methods boolean int long float double bytes string))

(defmethod compare ((schema fixed-schema)
                    (left stream)
                    (right stream))
  (compare-fixed schema left right))

(defmethod compare ((schema array-schema)
                    (left stream)
                    (right stream))
  (compare-array schema left right))

(defmethod compare ((schema enum-schema)
                    (left stream)
                    (right stream))
  (compare-enum left right))

(defmethod compare ((schema union-schema)
                    (left stream)
                    (right stream))
  (compare-union schema left right))

(defmethod compare ((schema record-schema)
                    (left stream)
                    (right stream))
  (compare-record schema left right))
