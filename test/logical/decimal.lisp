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

(defpackage #:test/decimal
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:define-schema-test
                #:json-syntax))

(in-package #:test/decimal)

(named-readtables:in-readtable json-syntax)

;;; underlying bytes

;; TODO fall-through

#+nil
(let* ((expected-precision 3)
       (expected-scale (1+ expected-precision))
       (json (make-json expected-precision expected-scale))
       (schema (avro:deserialize 'avro:schema json)))
  (is (eq 'avro:bytes schema)))

(define-schema-test schema-bytes
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 2
  }
  {
    "precision": 4,
    "scale": 2,
    "type": "bytes",
    "logicalType": "decimal"
  }
  #x5452f11fed1af467
  (make-instance
   'avro:decimal
   :underlying 'avro:bytes
   :precision 4
   :scale 2)
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying avro:bytes)
    (:precision 4)
    (:scale 2)))

(test io-bytes
  (let* ((expected-precision 4)
         (expected-scale 2)
         (schema (make-instance
                  'avro:decimal
                  :underlying 'avro:bytes
                  :precision expected-precision
                  :scale expected-scale))
         (expected-unscaled -1234)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 3 :element-type '(unsigned-byte 8)
                                   :initial-contents '(4 #xfb #x2e))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled -12340)))))

(define-schema-test schema-bytes-same-scale-and-precision
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 4
  }
  {
    "precision": 4,
    "scale": 4,
    "type": "bytes",
    "logicalType": "decimal"
  }
  #x7a2017da52b0b0d
  (make-instance
   'avro:decimal
   :underlying 'avro:bytes
   :precision 4
   :scale 4)
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying avro:bytes)
    (:precision 4)
    (:scale 4)))

(test io-bytes-same-scale-and-precision
  (let* ((expected-precision 4)
         (expected-scale expected-precision)
         (schema (make-instance
                  'avro:decimal
                  :underlying 'avro:bytes
                  :precision expected-precision
                  :scale expected-scale))
         (expected-unscaled 1234)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 3 :element-type '(unsigned-byte 8)
                                   :initial-contents '(4 #x04 #xd2))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled 12340)))))

(define-schema-test schema-bytes-default-scale
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 3,
  }
  {
    "precision": 3,
    "type": "bytes",
    "logicalType": "decimal"
  }
  #x6d0f5de0928faf67
  (make-instance
   'avro:decimal
   :underlying 'avro:bytes
   :precision 3)
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying avro:bytes)
    (:precision 3)))

(test io-bytes-default-scale
  (let* ((expected-precision 3)
         (schema (make-instance
                  'avro:decimal
                  :underlying 'avro:bytes
                  :precision expected-precision))
         (expected-unscaled -123)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 2 :element-type '(unsigned-byte 8)
                                   :initial-contents '(2 #x85))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled -1230)))))

;;; fixed underlying

;; TODO fall-through

#+nil
(let* ((expected-precision 3)
       (expected-scale (1+ expected-precision))
       (expected-size 2)
       (json (make-json expected-precision expected-size expected-scale))
       (schema (avro:deserialize 'avro:schema json)))
  (is (typep schema 'avro:fixed))
  (is (= expected-size (avro:size schema))))

#+nil
(let* ((expected-precision 3)
       (expected-scale 2)
       (expected-size 1)
       (json (make-json expected-precision expected-size expected-scale))
       (schema (avro:deserialize 'avro:schema json)))
  (is (typep schema 'avro:fixed))
  (is (= expected-size (avro:size schema))))

(define-schema-test schema-fixed
  {
    "type": {
      "type": "fixed",
      "name": "foo",
      "size": 2
    },
    "logicalType": "decimal",
    "precision": 4,
    "scale": 2
  }
  {
    "precision": 4,
    "scale": 2,
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 2
    },
    "logicalType": "decimal"
  }
  #xdbaa0878f079e119
  (make-instance
   'avro:decimal
   :underlying (make-instance
                'avro:fixed
                :name "foo"
                :size 2)
   :precision 4
   :scale 2)
  (defclass |foo| ()
    ()
    (:metaclass avro:fixed)
    (:size 2))
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying |foo|)
    (:precision 4)
    (:scale 2)))

(test io-fixed
  (let* ((expected-precision 4)
         (expected-scale 2)
         (schema (make-instance
                  'avro:decimal
                  :underlying (make-instance
                               'avro:fixed
                               :name "foo"
                               :size 2)
                  :precision expected-precision
                  :scale expected-scale))
         (expected-unscaled -1234)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 2 :element-type '(unsigned-byte 8)
                                   :initial-contents '(#xfb #x2e))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled -12340)))))

(define-schema-test schema-fixed-same-scale-and-precision
  {
    "type": {
      "type": "fixed",
      "name": "foo",
      "size": 4
    },
    "logicalType": "decimal",
    "precision": 4,
    "scale": 4
  }
  {
    "precision": 4,
    "scale": 4,
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 4
    },
    "logicalType": "decimal"
  }
  #x318a6202499b604b
  (make-instance
   'avro:decimal
   :underlying (make-instance
                'avro:fixed
                :name "foo"
                :size 4)
   :precision 4
   :scale 4)
  (defclass |foo| ()
    ()
    (:metaclass avro:fixed)
    (:size 4))
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying |foo|)
    (:precision 4)
    (:scale 4)))

(test io-fixed-same-scale-and-precision
  (let* ((expected-precision 4)
         (expected-scale expected-precision)
         (schema (make-instance
                  'avro:decimal
                  :underlying (make-instance
                               'avro:fixed
                               :name "foo"
                               :size 4)
                  :precision expected-precision
                  :scale expected-scale))
         (expected-unscaled 1234)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 4 :element-type '(unsigned-byte 8)
                                   :initial-contents '(#x00 #x00 #x04 #xd2))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled 12340)))))

(define-schema-test schema-fixed-default-scale
  {
    "type": {
      "type": "fixed",
      "name": "foo",
      "size": 4
    },
    "logicalType": "decimal",
    "precision": 4,
  }
  {
    "precision": 4,
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 4
    },
    "logicalType": "decimal"
  }
  #xfd7421f66a728ed0
  (make-instance
   'avro:decimal
   :underlying (make-instance
                'avro:fixed
                :name "foo"
                :size 4)
   :precision 4)
  (defclass |foo| ()
    ()
    (:metaclass avro:fixed)
    (:size 4))
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying |foo|)
    (:precision 4)))

(test io-fixed-default-scale
  (let* ((expected-precision 3)
         (schema (make-instance
                  'avro:decimal
                  :underlying (make-instance
                               'avro:fixed
                               :name "foo"
                               :size 4)
                  :precision expected-precision))
         (expected-unscaled -123)
         (object (make-instance schema :unscaled expected-unscaled))
         (serialized (make-array 4 :element-type '(unsigned-byte 8)
                                   :initial-contents '(#xff #xff #xff #x85))))
    (flet ((check (object)
             (is (= expected-unscaled (avro:unscaled object)))))
      (check object)
      (is (equalp serialized (avro:serialize object)))
      (let ((deserialized (avro:deserialize schema serialized)))
        (is (eq schema (avro:schema-of deserialized)))
        (check deserialized))
      (signals error
        (make-instance schema :unscaled -1230)))))
