;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro/test/resolution/record
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:cl-avro)))
(in-package #:cl-avro/test/resolution/record)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<float> ()
  ()
  (:metaclass avro:array)
  (:items avro:float))

(defclass writer_schema? ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null writer_schema))

(defclass writer_schema ()
  ((nums
    :type array<int>
    :initarg :nums
    :reader nums)
   (num
    :type avro:int
    :initarg :num
    :reader num)
   (str
    :type avro:string
    :initarg :str
    :reader str)
   (record
    :type writer_schema?
    :initarg :record
    :reader record)
   (extra
    :type avro:boolean
    :initarg :extra
    :reader extra))
  (:metaclass avro:record)
  (:default-initargs
   :record (make-instance 'writer_schema? :object nil)))

(defclass reader_schema? ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null reader_schema))

(defclass reader_schema ()
  ((reader_num
    :type avro:long
    :reader reader_num
    :aliases ("NUM"))
   (reader_nums
    :type array<float>
    :reader reader_nums
    :aliases ("NUMS"))
   (str
    :type avro:string
    :reader str)
   (reader_record
    :type reader_schema?
    :reader reader_record
    :aliases ("RECORD"))
   (exclusive
    :type avro:string
    :reader exclusive
    :default "foo"))
  (:metaclass avro:record)
  (:aliases "WRITER_SCHEMA"))

(defclass reader_schema_no_default ()
  ((reader_nums
    :type array<float>
    :reader reader_nums
    :aliases ("NUMS"))
   (str
    :type avro:string
    :reader str)
   (reader_num
    :type avro:long
    :reader reader_num
    :aliases ("NUM"))
   (reader_record
    :type reader_schema?
    :reader reader_record
    :aliases ("RECORD"))
   (exclusive
    :type avro:string
    :reader exclusive))
  (:metaclass avro:record)
  (:aliases "WRITER_SCHEMA"))

(test record->record
  (let* ((writer
           (make-instance
            'writer_schema
            :nums (make-instance 'array<int> :initial-contents '(2 4 6))
            :num 8
            :str "foo"
            :extra 'avro:true
            :record (make-instance
                     'writer_schema?
                     :object (make-instance
                              'writer_schema
                              :nums (make-instance
                                     'array<int> :initial-contents '(8 10 12))
                              :num 14
                              :str "bar"
                              :extra 'avro:false))))
         (reader
           (avro:coerce
            (avro:deserialize 'writer_schema (avro:serialize writer))
            'reader_schema)))
    (is (typep writer 'writer_schema))
    (is (typep reader 'reader_schema))

    (is (every #'= (nums writer) (reader_nums reader)))
    (is (= (num writer) (reader_num reader)))
    (is (string= (str writer) (str reader)))
    (is (string= "foo" (exclusive reader)))

    (let* ((writer? (record writer))
           (reader? (reader_record reader))
           (writer (avro:object writer?))
           (reader (avro:object reader?)))
      (is (typep writer? 'writer_schema?))
      (is (typep reader? 'reader_schema?))
      (is (typep writer 'writer_schema))
      (is (typep reader 'reader_schema))

      (is (every #'= (nums writer) (reader_nums reader)))
      (is (= (num writer) (reader_num reader)))
      (is (string= (str writer) (str reader)))
      (is (string= "foo" (exclusive reader)))

      (let* ((writer? (record writer))
             (reader? (reader_record reader))
             (writer (avro:object writer?))
             (reader (avro:object reader?)))
        (is (typep writer? 'writer_schema?))
        (is (typep reader? 'reader_schema?))
        (is (typep writer 'avro:null))
        (is (typep reader 'avro:null))))))

(test no-default
  (let ((writer
          (make-instance
           'writer_schema
           :nums (make-instance 'array<int> :initial-contents '(2 4 6))
           :num 8
           :str "foo"
           :extra 'avro:true
           :record (make-instance
                    'writer_schema?
                    :object (make-instance
                             'writer_schema
                             :nums (make-instance
                                    'array<int> :initial-contents '(8 10 12))
                             :num 14
                             :str "bar"
                             :extra 'avro:false)))))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer_schema (avro:serialize writer))
       'reader_schema_no_default))))
