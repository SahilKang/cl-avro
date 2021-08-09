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
(defpackage #:test/resolution/decimal
  (:use #:cl #:1am))
(in-package #:test/resolution/decimal)

;;; fixed underlying

(defclass fixed_size_2 ()
  ()
  (:metaclass avro:fixed)
  (:size 2))

(defclass fixed_size_3 ()
  ()
  (:metaclass avro:fixed)
  (:size 3))

;;; writer schema

(defclass writer-fixed ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying fixed_size_2))

(defclass writer-bytes ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying avro:bytes))

;;; reader schema

(defclass reader-fixed ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying fixed_size_3))

(defclass reader-bytes ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying avro:bytes))

;;; mismatched precision/scale

(defclass bad-precision ()
  ()
  (:metaclass avro:decimal)
  (:precision 2)
  (:scale 2)
  (:underlying avro:bytes))

(defclass bad-scale ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 1)
  (:underlying avro:bytes))

(test fixed->fixed
  (let* ((writer (make-instance 'writer-fixed :unscaled 123))
         (reader (avro:deserialize
                  'writer-fixed
                  (avro:serialize writer)
                  :reader-schema 'reader-fixed)))
    (is (typep writer 'writer-fixed))
    (is (typep reader 'reader-fixed))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test fixed->bytes
  (let* ((writer (make-instance 'writer-fixed :unscaled 123))
         (reader (avro:deserialize
                  'writer-fixed
                  (avro:serialize writer)
                  :reader-schema 'reader-bytes)))
    (is (typep writer 'writer-fixed))
    (is (typep reader 'reader-bytes))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test bytes->bytes
  (let* ((writer (make-instance 'writer-bytes :unscaled 123))
         (reader (avro:deserialize
                  'writer-bytes
                  (avro:serialize writer)
                  :reader-schema 'reader-bytes)))
    (is (typep writer 'writer-bytes))
    (is (typep reader 'reader-bytes))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test bytes->fixed
  (let* ((writer (make-instance 'writer-bytes :unscaled 123))
         (reader (avro:deserialize
                  'writer-bytes
                  (avro:serialize writer)
                  :reader-schema 'reader-fixed)))
    (is (typep writer 'writer-bytes))
    (is (typep reader 'reader-fixed))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test mismatched-precision
  (let ((writer (make-instance 'writer-bytes :unscaled 123)))
    (signals error
      (avro:deserialize
       'writer-bytes
       (avro:serialize writer)
       :reader-schema 'bad-precision))))

(test mismatched-scale
  (let ((writer (make-instance 'writer-bytes :unscaled 123)))
    (signals error
      (avro:deserialize
       'writer-bytes
       (avro:serialize writer)
       :reader-schema 'bad-scale)))) 
