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

(defpackage #:test/object-container-file
  (:use #:cl #:1am))

(in-package #:test/object-container-file)

(defparameter *weather-filespec*
  (asdf:system-relative-pathname 'cl-avro/test "test/weather.avro"))

(test read-file
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with stream = (make-instance 'avro:file-input-stream
                                                 :input stream)
                    for block = (avro:read-block stream)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (avro:field rhs "station"))
                   (time (avro:field rhs "time"))
                   (temp (avro:field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test write-file
  (let ((bytes (make-instance 'avro::memory-output-stream)))
    (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
      (loop
        with in = (make-instance 'avro:file-input-stream :input stream)
        with out = (make-instance 'avro:file-output-stream
                                  :meta (make-instance
                                         'avro:meta
                                         :schema (avro:schema (avro:header in)))
                                  :output bytes)
        for block = (avro:read-block in)
        while block
        do (avro:write-block out block)))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with stream = (make-instance
                                   'avro:file-input-stream
                                   ;; TODO require this to be a stream as well
                                   :input (coerce
                                           (avro:bytes bytes)
                                           '(simple-array (unsigned-byte 8) (*))))
                    for block = (avro:read-block stream)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (avro:field rhs "station"))
                   (time (avro:field rhs "time"))
                   (temp (avro:field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test skip-block
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((stream (make-instance 'avro:file-input-stream :input stream)))
      (is (avro:skip-block stream))
      (is (null (avro:skip-block stream)))
      (is (null (avro:read-block stream))))))
