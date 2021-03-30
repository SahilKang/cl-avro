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
(defpackage #:cl-avro.io.resolution.resolve
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:deserialize-from-vector
                #:deserialize-from-stream
                #:define-deserialize-from
                #:assert-match
                #:resolve)
  (:export #:resolve
           #:resolved
           #:reader
           #:writer
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:cl-avro.io.resolution.resolve)

(defclass resolved ()
  ((reader
    :initarg :reader
    :reader reader
    :type schema:schema
    :documentation "Reader schema.")
   (writer
    :initarg :writer
    :reader writer
    :type schema:schema
    :documentation "Writer schema"))
  (:default-initargs
   :reader (error "Must supply READER")
   :writer (error "Must supply WRITER")))

;; array schema

(defmethod resolve
    ((reader schema:array) (writer schema:array))
  (let ((resolved (resolve (schema:items reader)
                           (schema:items writer))))
    (make-instance reader :items resolved)))

;; map schema

(defmethod resolve
    ((reader schema:map) (writer schema:map))
  (let ((resolved (resolve (schema:values reader)
                           (schema:values writer))))
    (make-instance reader :values resolved)))

;; enum schema

(defclass resolved-enum (resolved)
  ((known-symbols
    :reader known-symbols
    :type hash-table)
   (reader
    :type schema:enum)
   (writer
    :type schema:enum)))

(declaim
 (ftype (function (schema:enum) (values hash-table &optional))
        get-known-symbols)
 (inline get-known-symbols))
(defun get-known-symbols (enum)
  (loop
    with symbols of-type (simple-array schema:name (*)) = (schema:symbols enum)
    with known-symbols = (make-hash-table :test #'equal :size (length symbols))

    for symbol across symbols
    do (setf (gethash symbol known-symbols) t)

    finally
       (return known-symbols)))
(declaim (notinline get-known-symbols))

(defmethod initialize-instance :after
    ((instance resolved-enum) &key)
  (declare (inline get-known-symbols))
  (with-slots (reader writer known-symbols) instance
    (setf known-symbols (get-known-symbols reader))))

(defmethod resolve
    ((reader schema:enum) (writer schema:enum))
  (make-instance 'resolved-enum :reader reader :writer writer))

(define-deserialize-from resolved-enum
  `(multiple-value-bind (enum bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (let ((known-symbols (known-symbols schema))
           (default (schema:default (reader schema)))
           (symbol (schema:which-one enum)))
       (unless (or (gethash symbol known-symbols)
                   default)
         (error "Reader enum has no default for unknown writer symbol ~S" symbol))
       (values
        (make-instance
         (reader schema)
         :enum (if (gethash symbol known-symbols)
                   symbol
                   default))
        bytes-read))))

;; record schema

(defclass resolved-record (resolved)
  ((defaulted-initargs
    :initform nil
    :reader defaulted-initargs
    :type list)
   (needed-fields
    :initform (make-array 0 :element-type 'schema:name
                            :adjustable t :fill-pointer t)
    :reader needed-fields
    :type (vector schema:name))
   (reader
    :type schema:record)
   (writer
    :type schema:record)))

(declaim
 (ftype (function (schema:record schema:record)
                  (values schema:record &optional))
        resolve-records)
 (inline resolve-records))
(defun resolve-records (reader writer)
  (loop
    with name->reader-field = (schema:name->field reader)

    for writer-field across (schema:fields writer)
    for writer-type = (schema:type writer-field)
    for reader-field
      = (or (gethash (schema:name writer-field) name->reader-field)
            (some (lambda (alias)
                    (gethash alias name->reader-field))
                  (schema:aliases writer-field)))

    if reader-field
      collect (let* ((reader-type (schema:type reader-field))
                     (resolved-type (resolve reader-type writer-type)))
                (list
                 :name (make-symbol ;; TODO just use second value
                        (the schema:name (schema:name reader-field)))
                 :type resolved-type))
        into slots
    else
      collect (list
               :name (make-symbol
                      (the schema:name (schema:name writer-field)))
               :type writer-type)
        into slots

    finally
       (return (make-instance
                'schema:record
                :name (schema:name writer)
                :direct-slots slots))))
(declaim (notinline resolve-records))

(defmethod initialize-instance :after
    ((instance resolved-record) &key)
  (declare (inline resolve-records))
  (with-slots (reader writer defaulted-initargs needed-fields) instance
    (loop
      with name->writer-field = (schema:name->field writer)

      for reader-field across (schema:fields reader)
      for writer-field
        = (or (gethash (schema:name reader-field) name->writer-field)
              (some (lambda (alias)
                      (gethash alias name->writer-field))
                    (schema:aliases reader-field)))

      if writer-field do
        (vector-push-extend (schema:name reader-field) needed-fields)
      else do
        (multiple-value-bind (default defaultp)
            (schema:default reader-field)
          (unless defaultp
            (error "Writer field ~S does not exist and reader has no default"
                   (schema:name reader-field)))
          (push default defaulted-initargs)
          (push (intern (schema:name reader-field) 'keyword) defaulted-initargs)))
    (setf writer (resolve-records reader writer))))

(defmethod resolve
    ((reader schema:record) (writer schema:record))
  (make-instance 'resolved-record :reader reader :writer writer))

(declaim
 (ftype (function (resolved-record schema:record-object)
                  (values list &optional))
        make-initargs)
 (inline make-initargs))
(defun make-initargs (schema object)
  (loop
    with initargs = (defaulted-initargs schema)

    for name across (needed-fields schema)
    for value = (schema:field object name)

    do
       (push value initargs)
       (push (intern name 'keyword) initargs)

    finally
       (return initargs)))
(declaim (notinline make-initargs))

(define-deserialize-from resolved-record
  (declare (inline make-initargs))
  `(multiple-value-bind (object bytes-read)
       ,(if vectorp
            `(deserialize-from-vector (writer schema) vector start)
            `(deserialize-from-stream (writer schema) stream))
     (let ((initargs (make-initargs schema object)))
       (values
        (apply #'make-instance (reader schema) initargs)
        bytes-read))))

;; union schema

(defclass resolved-union (resolved)
  ((reader
    :type schema:union)
   (writer
    :type schema:union)))

(declaim
 (ftype (function (schema:schema schema:schema) (values boolean &optional))
        matchp)
 (inline matchp))
(defun matchp (reader writer)
  (handler-case
      (progn
        (assert-match reader writer)
        t)
    (error ()
      nil)))
(declaim (notinline matchp))

(defmethod resolve
    ((reader schema:union) (writer schema:union))
  (make-instance 'resolved-union :reader reader :writer writer))

(defmethod resolve
    ((reader schema:union) writer)
  (declare (inline matchp))
  (check-type writer schema:schema)
  (let* ((reader-schemas (schema:schemas reader))
         (first-match (find-if (lambda (reader-schema)
                                 (matchp reader-schema writer))
                               reader-schemas)))
    (declare ((simple-array schema:schema (*)) reader-schemas))
    (unless first-match
      (error "None of the reader union's schemas match the writer schema"))
    (resolve first-match writer)))

(defmethod resolve
    (reader (writer schema:union))
  (let ((reader-union (make-instance 'schema:union :schemas reader)))
    (make-instance 'resolved-union :reader reader-union :writer writer)))

(define-deserialize-from resolved-union
  (declare (inline matchp))
  `(let* ((total-bytes-read 0)
          (writer-schema
            (let ((schemas (schema:schemas (writer schema))))
              (declare ((simple-array schema:schema (*)) schemas))
              (multiple-value-bind (position bytes-read)
                  ,(if vectorp
                       `(deserialize-from-vector 'schema:int vector start)
                       `(deserialize-from-stream 'schema:int stream))
                (incf total-bytes-read bytes-read)
                (elt schemas position))))
          (reader-schemas (schema:schemas (reader schema)))
          (first-match
            (find-if (lambda (reader-schema)
                       (matchp reader-schema writer-schema))
                     reader-schemas)))
     (declare ((simple-array schema:schema (*)) reader-schemas))
     (unless first-match
       ,@(when streamp
           ;; consume stream
           `((deserialize-from-stream writer-schema stream)))
       (error "None of the reader union's schemas match the writer schema"))
     (let ((resolved-schema (resolve first-match writer-schema)))
       (multiple-value-bind (value bytes-read)
           ,(if vectorp
                `(deserialize-from-vector resolved-schema vector (+ start total-bytes-read))
                `(deserialize-from-stream resolved-schema stream))
         (incf total-bytes-read bytes-read)
         (values (make-instance (reader schema) :object value) total-bytes-read)))))
