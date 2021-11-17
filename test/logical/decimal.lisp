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
(defpackage #:cl-avro/test/decimal
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)
   (#:internal #:cl-avro.internal))
  (:import-from #:cl-avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:cl-avro/test/decimal)

(named-readtables:in-readtable json-syntax)

;;; underlying bytes

;; TODO fall-through

#+nil
(let* ((expected-precision 3)
       (expected-scale (1+ expected-precision))
       (json (make-json expected-precision expected-scale))
       (schema (avro:deserialize 'avro:schema json)))
  (is (eq 'avro:bytes schema)))

;; TODO canonical form isn't specified for logical schemas
(define-schema-test schema-bytes
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 2
  }
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 2
  }
  #xeeb4ee37786d146e
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

(define-io-test io-bytes
    ((precision 4)
     (scale 2)
     (unscaled -1234))
    (make-instance
     'avro:decimal
     :underlying 'avro:bytes
     :precision precision
     :scale scale)
    (make-instance schema :unscaled unscaled)
    (4 #xfb #x2e)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled -12340)))

(define-schema-test schema-bytes-same-scale-and-precision
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 4
  }
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 4,
    "scale": 4
  }
  #xf07f87a02e62b644
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

(define-io-test io-bytes-same-scale-and-precision
    ((precision 4)
     (scale precision)
     (unscaled 1234))
    (make-instance
     'avro:decimal
     :underlying 'avro:bytes
     :precision precision
     :scale scale)
    (make-instance schema :unscaled unscaled)
    (4 #x04 #xd2)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled 12340)))

(define-schema-test schema-bytes-default-scale
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 3,
  }
  {
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 3
  }
  #x69151c93bbffa7b8
  (make-instance
   'avro:decimal
   :underlying 'avro:bytes
   :precision 3)
  (defclass #:decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying avro:bytes)
    (:precision 3)))

(define-io-test io-bytes-default-scale
    ((precision 3)
     (unscaled -123))
    (make-instance 'avro:decimal :underlying 'avro:bytes :precision precision)
    (make-instance schema :unscaled unscaled)
    (2 #x85)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled -1230)))

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
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 2
    },
    "logicalType": "decimal",
    "precision": 4,
    "scale": 2
  }
  #x38262a1302a1557f
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

(define-io-test io-fixed
    ((precision 4)
     (scale 2)
     (unscaled -1234))
    (make-instance
     'avro:decimal
     :underlying (make-instance 'avro:fixed :name "foo" :size 2)
     :precision precision
     :scale scale)
    (make-instance schema :unscaled unscaled)
    (#xfb #x2e)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled -12340)))

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
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 4
    },
    "logicalType": "decimal",
    "precision": 4,
    "scale": 4
  }
  #x6cc413011b16903f
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

(define-io-test io-fixed-same-scale-and-precision
    ((precision 4)
     (scale precision)
     (unscaled 1234))
    (make-instance
     'avro:decimal
     :underlying (make-instance 'avro:fixed :name "foo" :size 4)
     :precision precision
     :scale scale)
    (make-instance schema :unscaled unscaled)
    (#x00 #x00 #x04 #xd2)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled 12340)))

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
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 4
    },
    "logicalType": "decimal",
    "precision": 4
  }
  #xa67f01d3bbb18067
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

(define-io-test io-fixed-default-scale
    ((precision 3)
     (unscaled -123))
    (make-instance
     'avro:decimal
     :underlying (make-instance 'avro:fixed :name "foo" :size 4)
     :precision precision)
    (make-instance schema :unscaled unscaled)
    (#xff #xff #xff #x85)
  (is (= unscaled (avro:unscaled arg)))
  (signals error
    (make-instance schema :unscaled -1230)))

(test late-type-check
  (setf (find-class 'late_decimal) nil
        (find-class 'late_fixed) nil)

  (defclass late_decimal ()
    ()
    (:metaclass avro:decimal)
    (:underlying late_fixed)
    (:precision 4))

  (signals error
    (avro:precision (find-class 'late_decimal)))
  (signals error
    (avro:scale (find-class 'late_decimal)))
  (signals error
    ;; TODO maybe expose this publicly
    (internal:underlying (find-class 'late_decimal)))

  (defclass late_fixed ()
    ()
    (:metaclass avro:fixed)
    (:size 12))

  (is (= 4 (avro:precision (find-class 'late_decimal))))
  (is (zerop (avro:scale (find-class 'late_decimal))))
  (is (eq (find-class 'late_fixed) (internal:underlying (find-class 'late_decimal)))))

(test scale-greater-than-precision
  (signals error
    (make-instance
     'avro:decimal
     :precision 2
     :scale 3
     :underlying 'avro:bytes)))

(test indecent-precision
  (let ((schema (make-instance
                 'avro:decimal
                 :precision 7
                 :underlying (make-instance
                              'avro:fixed
                              :name "foo"
                              :size 3))))
    (signals error
      (avro:precision schema))))

(test size-zero-fixed
  (let ((schema (make-instance
                 'avro:decimal
                 :precision 1
                 :underlying (make-instance
                              'avro:fixed
                              :name "foo"
                              :size 0))))
    (signals error
      (avro:precision schema))))
