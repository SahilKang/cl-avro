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

(in-package #:cl-user)

(defpackage #:test/object-container-file
  (:use #:cl #:1am))

(in-package #:test/object-container-file)

(test read-file
  (with-open-file (stream "./weather.avro" :element-type '(unsigned-byte 8))
    (let ((expected '(#("011990-99999" -619524000000 0)
                      #("011990-99999" -619506000000 22)
                      #("011990-99999" -619484400000 -11)
                      #("012650-99999" -655531200000 111)
                      #("012650-99999" -655509600000 78)))
          (actual (loop
                     with records = nil
                     with stream = (make-instance 'avro:file-input-stream
                                                  :stream-or-seq stream)
                     for block = (avro:read-block stream)
                     until (eq block :eof)
                     do (setf records (concatenate 'list records block))
                     finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (is (equalp lhs rhs)))
           expected
           actual))))

(test write-file
  (let ((bytes (make-array 0
                           :element-type '(unsigned-byte 8)
                           :adjustable t
                           :fill-pointer 0)))
    (with-open-file (stream "./weather.avro" :element-type '(unsigned-byte 8))
      (loop
         with in = (make-instance 'avro:file-input-stream :stream-or-seq stream)
         with out = (make-instance 'avro:file-output-stream
                                   :schema (avro:schema in)
                                   :stream-or-vector bytes)
         for block = (avro:read-block in)
         until (eq block :eof)
         do (avro:write-block out block)))
    (let ((expected '(#("011990-99999" -619524000000 0)
                      #("011990-99999" -619506000000 22)
                      #("011990-99999" -619484400000 -11)
                      #("012650-99999" -655531200000 111)
                      #("012650-99999" -655509600000 78)))
          (actual (loop
                     with records = nil
                     with stream = (make-instance 'avro:file-input-stream
                                                  :stream-or-seq bytes)
                     for block = (avro:read-block stream)
                     until (eq block :eof)
                     do (setf records (concatenate 'list records block))
                     finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (is (equalp lhs rhs)))
           expected
           actual))))

(test skip-block
  (with-open-file (stream "./weather.avro" :element-type '(unsigned-byte 8))
    (let ((stream (make-instance 'avro:file-input-stream :stream-or-seq stream)))
      (is (avro:skip-block stream))
      (is (null (avro:skip-block stream)))
      (is (eq :eof (avro:read-block stream))))))
