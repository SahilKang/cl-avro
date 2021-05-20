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

(defpackage #:test/long
  (:use #:cl #:1am)
  (:import-from #:test/common
                #:json-syntax))

(in-package #:test/long)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "long"})
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= "\"long\"" (avro:serialize 'avro:long)))
    (is (= fingerprint (avro:fingerprint64 'avro:long)))))

(test canonical-form
  (let ((json "\"long\"")
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:long :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint64 'avro:long)))))

(test io
  (loop
    with long->serialized
      = '((0 . (#x00))
          (-1 . (#x01))
          (1 . (#x02))
          (-2 . (#x03))
          (2 . (#x04))
          (-64 . (#x7f))
          (64 . (#x80 #x01))
          (8192 . (#x80 #x80 #x01))
          (-8193 . (#x81 #x80 #x01)))
        initially
           (dolist (cons long->serialized)
             (let* ((bytes-list (cdr cons))
                    (bytes-vector (make-array
                                   (length bytes-list)
                                   :element-type '(unsigned-byte 8)
                                   :initial-contents bytes-list)))
               (rplacd cons bytes-vector)))

    for (long . serialized) in long->serialized
    do
       (is (equalp serialized (avro:serialize long)))
       (is (= long (avro:deserialize 'avro:long serialized)))))

(test io-edges
  (let ((max (1- (expt 2 63)))
        (min (- (expt 2 63))))
    (is (typep max 'avro:long))
    (is (typep min 'avro:long))

    (is (not (typep (1+ max) 'avro:long)))
    (is (not (typep (1- min) 'avro:long)))

    (is (= max (avro:deserialize 'avro:long (avro:serialize max))))
    (is (= min (avro:deserialize 'avro:long (avro:serialize min))))

    (signals error
      (avro:serialize (1+ max)))
    (signals error
      (avro:serialize (1- min)))))
