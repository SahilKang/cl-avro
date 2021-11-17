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
(defpackage #:cl-avro.internal.ieee-754
  (:use #:cl)
  (:import-from #:cl-avro.internal.type
                #:vector<uint8>
                #:ufixnum
                #:comparison)
  (:import-from #:cl-avro.internal.compare
                #:compare-reals)
  (:export #:implement))
(in-package #:cl-avro.internal.ieee-754)

;;; serialize

(defmacro define-serialize-into (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "SERIALIZE-INTO-~A" vector/stream)))
         (float/double (if (= bits 32) 'single-float 'double-float))
         (size (/ bits 8))
         (size-type `(integer ,size ,size))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (encode-float (multiple-value-bind (symbol status)
                           (find-symbol
                            (format nil "ENCODE-FLOAT~A" bits)
                            'ieee-floats)
                         (assert (eq status :external))
                         symbol))
         (to-little-endian (multiple-value-bind (symbol status)
                               (find-symbol
                                (format nil "UINT~A->~A" bits vector/stream)
                                'cl-avro.internal.little-endian)
                             (assert (eq status :external))
                             symbol)))
    `(progn
       (declaim
        (ftype (function (,float/double ,@arg-types)
                         (values ,size-type &optional))
               ,name))
       (defun ,name (object ,@args)
         (,to-little-endian (,encode-float object) ,@args)
         ,size))))

(defmacro define-serialize-intos (bits)
  `(progn
     (define-serialize-into ,bits vector)
     (define-serialize-into ,bits stream)))

;;; deserialize

(defmacro define-deserialize-from (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "DESERIALIZE-FROM-~A" vector/stream)))
         (float/double (if (= bits 32) 'single-float 'double-float))
         (size (/ bits 8))
         (size-type `(integer ,size ,size))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (return-type `(values ,float/double ,size-type &optional))
         (decode-float (multiple-value-bind (symbol status)
                           (find-symbol
                            (format nil "DECODE-FLOAT~A" bits)
                            'ieee-floats)
                         (assert (eq status :external))
                         symbol))
         (from-little-endian (multiple-value-bind (symbol status)
                                 (find-symbol
                                  (format nil "~A->UINT~A" vector/stream bits)
                                  'cl-avro.internal.little-endian)
                               (assert (eq status :external))
                               symbol)))
    `(progn
       (declaim (ftype (function ,arg-types ,return-type) ,name))
       (defun ,name ,args
         (values (,decode-float (,from-little-endian ,@args)) ,size)))))

(defmacro define-deserialize-froms (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-deserialize-from ,bits vector)
     (define-deserialize-from ,bits stream)))

;;; compare

(defmacro define-compare (vector/stream)
  (declare ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "COMPARE-~AS" vector/stream)))
         (deserialize (intern
                       (format nil "DESERIALIZE-FROM-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp
                        '(vector<uint8> vector<uint8> ufixnum ufixnum)
                        '(stream stream)))
         (args (when vectorp '(left-start right-start)))
         (left-arg (when vectorp (list (car args))))
         (right-arg (when vectorp (cdr args))))
    `(progn
       (declaim
        (ftype (function ,arg-types
                         (values comparison ufixnum ufixnum &optional))
               ,name))
       (defun ,name (left right ,@args)
         (multiple-value-bind (left left-bytes-read)
             (,deserialize left ,@left-arg)
           (multiple-value-bind (right right-bytes-read)
               (,deserialize right ,@right-arg)
             (values (compare-reals left right)
                     left-bytes-read
                     right-bytes-read)))))))

(defmacro define-compares ()
  `(progn
     (define-compare vector)
     (define-compare stream)))

;;; implement

(defmacro implement (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-serialize-intos ,bits)
     (define-deserialize-froms ,bits)
     (define-compares)))
