;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro/test/file
  (:local-nicknames
   (#:avro #:cl-avro))
  (:use #:cl #:1am))
(in-package #:cl-avro/test/file)

(defparameter *weather-filespec*
  (asdf:system-relative-pathname 'cl-avro/test "test/weather.avro"))

(declaim
 (ftype (function (avro:record-object simple-string)
                  (values avro:object &optional))
        field))
(defun field (record field)
  (let ((found-field (find field (avro:fields (class-of record))
                           :key #'avro:name :test #'string=)))
    (unless found-field
      (error "No such field ~S" field))
    (slot-value record (nth-value 1 (avro:name found-field)))))

(test read-file
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with reader = (make-instance 'avro:file-reader :input stream)
                    for block = (avro:read-block reader)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (field rhs "station"))
                   (time (field rhs "time"))
                   (temp (field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test write-file
  (let ((bytes (flexi-streams:make-in-memory-output-stream)))
    (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
      (loop
        with reader = (make-instance 'avro:file-reader :input stream)
        with writer = (make-instance 'avro:file-writer
                                     :meta (make-instance
                                            'avro:meta
                                            :schema (avro:schema
                                                     (avro:file-header reader)))
                                     :output bytes)
        for block = (avro:read-block reader)
        while block
        do (avro:write-block writer block)))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with reader = (make-instance
                                   'avro:file-reader
                                   :input (flexi-streams:get-output-stream-sequence bytes))
                    for block = (avro:read-block reader)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (field rhs "station"))
                   (time (field rhs "time"))
                   (temp (field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test skip-block
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((reader (make-instance 'avro:file-reader :input stream)))
      (is (avro:skip-block reader))
      (is (null (avro:skip-block reader)))
      (is (null (avro:read-block reader))))))
