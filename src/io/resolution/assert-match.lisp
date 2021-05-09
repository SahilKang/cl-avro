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
(defpackage #:cl-avro.io.resolution.assert-match
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:assert-match)
  (:export #:assert-match))
(in-package #:cl-avro.io.resolution.assert-match)

;;; assert-matching-names

(declaim
 (ftype (function (schema:name schema:fullname) (values boolean &optional))
        matching-alias-p)
 (inline matching-alias-p))
(defun matching-alias-p (writer-name reader-alias)
  (declare (inline schema:fullname->name))
  (string= writer-name (schema:fullname->name reader-alias)))
(declaim (notinline matching-alias-p))

(declaim
 (ftype (function (schema:name (simple-array schema:fullname (*)))
                  (values boolean &optional))
        matching-aliases-p)
 (inline matching-aliases-p))
(defun matching-aliases-p (writer-name reader-aliases)
  (declare (inline matching-alias-p))
  (flet ((matching-alias-p (reader-alias)
           (matching-alias-p writer-name reader-alias)))
    (not (null (some #'matching-alias-p reader-aliases)))))
(declaim (notinline matching-aliases-p))

(declaim
 (ftype (function (schema:named-schema schema:named-schema)
                  (values boolean &optional))
        matching-names-p)
 (inline matching-names-p))
(defun matching-names-p (reader writer)
  (declare (inline matching-aliases-p))
  (let ((reader-name (schema:name reader))
        (writer-name (schema:name writer)))
    (declare (schema:name reader-name writer-name))
    (or (string= reader-name writer-name)
        (matching-aliases-p writer-name (schema:aliases reader)))))
(declaim (notinline matching-names-p))

(declaim
 (ftype (function (schema:named-schema schema:named-schema) (values &optional))
        assert-matching-names)
 (inline assert-matching-names))
(defun assert-matching-names (reader writer)
  (declare (inline matching-names-p))
  (unless (matching-names-p reader writer)
    (error "Names don't match between reader schema ~S and writer schema ~S"
           reader writer))
  (values))
(declaim (notinline assert-matching-names))

;; primitive schemas

(macrolet
    ((defmethods ()
       (flet ((make-defmethod (schema)
                `(defmethod assert-match
                     ((reader (eql ',schema))
                      (writer (eql ',schema)))
                   (declare (ignore reader writer))
                   (values))))
         (let ((primitives (mapcar #'car schema:+primitive->name+)))
           `(progn
              ,@(mapcar #'make-defmethod primitives))))))
  (defmethods))

;; array schema

(defmethod assert-match
    ((reader schema:array) (writer schema:array))
  (assert-match (schema:items reader) (schema:items writer)))

;; map schema

(defmethod assert-match
    ((reader schema:map) (writer schema:map))
  (assert-match (schema:values reader) (schema:values writer)))

;; enum schema

(defmethod assert-match
    ((reader schema:enum) (writer schema:enum))
  (declare (inline assert-matching-names))
  (assert-matching-names reader writer))

;; fixed schema

(defmethod assert-match
    ((reader schema:fixed) (writer schema:fixed))
  (declare (inline assert-matching-names))
  (let ((reader-size (schema:size reader))
        (writer-size (schema:size writer)))
    (declare ((integer 0) reader-size writer-size))
    (unless (= reader-size writer-size)
      (error "Reader and writer fixed schemas have different sizes: ~S and ~S"
             reader-size writer-size))
    (assert-matching-names reader writer)))

;; record schema

(defmethod assert-match
    ((reader schema:record) (writer schema:record))
  (declare (inline assert-matching-names))
  (assert-matching-names reader writer))

;; union schema

(defmethod assert-match
    ((reader schema:union) writer)
  (declare (ignore reader writer)))

(defmethod assert-match
    (reader (writer schema:union))
  (declare (ignore reader writer)))

;; logical schemas

(defmethod assert-match
    ((reader schema:logical-schema) writer)
  (assert-match (schema:underlying reader) writer))

(defmethod assert-match
    (reader (writer schema:logical-schema))
  (assert-match reader (schema:underlying writer)))

;; decimal schema

(defmethod assert-match
    ((reader schema:decimal) (writer schema:decimal))
  (let ((reader-scale (schema:scale reader))
        (writer-scale (schema:scale writer))
        (reader-precision (schema:precision reader))
        (writer-precision (schema:precision writer)))
    (declare ((integer 0) reader-scale writer-scale)
             ((integer 1) reader-precision writer-precision))
    (unless (= reader-scale writer-scale)
      (error "Reader and writer's decimal scales don't match: ~S and ~S"
             reader-scale writer-scale))
    (unless (= reader-precision writer-precision)
      (error "Reader and writer's decimal precisions don't match: ~S and ~S"
             reader-precision writer-precision))))

;; duration schema

(defmethod assert-match
    ((reader schema:duration) (writer schema:duration))
  (assert-match (schema:underlying reader) (schema:underlying writer)))
