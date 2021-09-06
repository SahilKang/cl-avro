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
(defpackage #:cl-avro.resolution.complex
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:base #:cl-avro.resolution.base)))
(in-package #:cl-avro.resolution.complex)

;;; assert-matching-names

(declaim
 (ftype (function (schema:name schema:fullname) (values boolean &optional))
        matching-alias-p))
(defun matching-alias-p (writer-name reader-alias)
  (string= writer-name (schema:fullname->name reader-alias)))

(declaim
 (ftype (function (schema:name (simple-array schema:fullname (*)))
                  (values boolean &optional))
        matching-aliases-p))
(defun matching-aliases-p (writer-name reader-aliases)
  (flet ((matching-alias-p (reader-alias)
           (matching-alias-p writer-name reader-alias)))
    (not (null (some #'matching-alias-p reader-aliases)))))

(declaim
 (ftype (function (schema:named-schema schema:named-schema)
                  (values boolean &optional))
        matching-names-p))
(defun matching-names-p (reader writer)
  (let ((reader-name (schema:name reader))
        (writer-name (schema:name writer)))
    (declare (schema:name reader-name writer-name))
    (or (string= reader-name writer-name)
        (matching-aliases-p writer-name (schema:aliases reader)))))

(declaim
 (ftype (function (schema:named-schema schema:named-schema) (values &optional))
        assert-matching-names))
(defun assert-matching-names (reader writer)
  (unless (matching-names-p reader writer)
    (error "Names don't match between reader schema ~S and writer schema ~S"
           reader writer))
  (values))

;;; fixed schema

(defmethod base:coerce
    ((object schema:fixed-object) (schema schema:fixed))
  (let* ((writer (class-of object))
         (writer-size (schema:size writer))
         (reader-size (schema:size schema)))
    (declare (schema:fixed writer)
             ((integer 0) reader-size writer-size))
    (assert-matching-names schema writer)
    (unless (= reader-size writer-size)
      (error "Reader and writer fixed schemas have different sizes: ~S and ~S"
             reader-size writer-size))
    (change-class object schema)))

;;; array schema

(defmethod base:coerce
    ((object schema:array-object) (schema schema:array))
  (let* ((items (schema:items schema))
         (type `(simple-array ,items (*))))
    (flet ((coerce-element (element)
             (base:coerce element items)))
      (make-instance schema :initial-contents (map type #'coerce-element object)))))

;;; map schema

(defmethod base:coerce
    ((object schema:map-object) (schema schema:map))
  (loop
    with raw-hash-table of-type hash-table = (schema:raw-hash-table object)
    and values of-type schema:schema = (schema:values schema)

    for key being the hash-keys of raw-hash-table
      using (hash-value value)

    do
       (setf (gethash key raw-hash-table) (base:coerce value values))

    finally
       (return
         (change-class object schema))))

;;; enum schema

(defmethod base:coerce
    ((object schema:enum-object) (schema schema:enum))
  (assert-matching-names schema (class-of object))
  (let* ((chosen-symbol (schema:which-one object))
         (reader-symbols (schema:symbols schema))
         (position (or (position chosen-symbol reader-symbols :test #'string=)
                       (nth-value 1 (schema:default schema)))))
    (declare (schema:name chosen-symbol)
             ((simple-array schema:name (*)) reader-symbols)
             ((or null cl-avro.schema.complex.enum::position) position))
    (unless position
      (error "Reader enum has no default for unknown writer symbol ~S"
             chosen-symbol))
    (setf (cl-avro.schema.complex.enum::position object) position))
  (change-class object schema))

;;; record schema

(declaim
 (ftype (function (schema:name (simple-array schema:field (*)))
                  (values (or null schema:field) &optional))
        find-field-by-name))
(defun find-field-by-name (name fields)
  (loop
    for field across fields
    when (string= name (schema:name field))
      return field))

(declaim
 (ftype (function ((or null (simple-array schema:name (*)))
                   (simple-array schema:field (*)))
                  (values (or null schema:field) &optional))
        find-field-by-alias))
(defun find-field-by-alias (aliases fields)
  (when aliases
    (loop
      for field across fields
      when (find (schema:name field) aliases :test #'string=)
        return field)))

(declaim
 (ftype (function (schema:field (simple-array schema:field (*)))
                  (values (or null schema:field) &optional))
        find-field))
(defun find-field (field fields)
  (or (find-field-by-name (schema:name field) fields)
      (find-field-by-alias (schema:aliases field) fields)))

(defmethod base:coerce
    ((object schema:record-object) (schema schema:record))
  (loop
    with writer of-type schema:record = (class-of object)
    with writer-fields = (schema:fields writer)
    and initargs = nil

          initially
             (assert-matching-names schema writer)

    for reader-field across (schema:fields schema)
    for initarg = (intern (schema:name reader-field) 'keyword)
    for writer-field = (find-field reader-field writer-fields)

    if writer-field do
      (let* ((writer-slot (nth-value 1 (schema:name writer-field)))
             (writer-value (slot-value object writer-slot))
             (reader-type (schema:type reader-field)))
        (push (base:coerce writer-value reader-type) initargs)
        (push initarg initargs))
    else do
      (multiple-value-bind (default defaultp)
          (schema:default reader-field)
        (unless defaultp
          (error "Writer field ~S does not exist and reader has not default"
                 (schema:name reader-field)))
        (push default initargs)
        (push initarg initargs))

    finally
       (return
         (apply #'change-class object schema initargs))))

;;; union schema

(declaim
 (ftype (function
         (schema:object (simple-array schema:schema (*)))
         (values schema:object (or null (and schema:int (integer 0))) &optional))
        first-coercion))
(defun first-coercion (object schemas)
  (loop
    for i below (length schemas)
    for schema = (elt schemas i)

    do
       (handler-case
           (base:coerce object schema)
         (error ())
         (:no-error (coerced)
           (return-from first-coercion
             (values coerced i))))

    finally
       (return
         (values nil nil))))

(defmethod base:coerce
    ((object schema:union-object) (schema schema:union))
  (multiple-value-bind (coerced position)
      (first-coercion (schema:object object) (schema:schemas schema))
    (unless position
      (error "None of the reader union's schemas match the chosen writer schema"))
    (let* ((wrapper-classes (cl-avro.schema.complex.union::wrapper-classes schema))
           (wrapper-class (elt wrapper-classes position))
           (wrapped-object (make-instance wrapper-class :wrap coerced)))
      (setf (cl-avro.schema.complex.union::wrapped-object object) wrapped-object)))
  (change-class object schema))

(defmethod base:coerce
    (object (schema schema:union))
  (multiple-value-bind (coerced position)
      (first-coercion object (schema:schemas schema))
    (unless position
      (error "None of the reader union's schemas match the writer schema"))
    (make-instance schema :object coerced)))

(defmethod base:coerce
    ((object schema:union-object) schema)
  (base:coerce (schema:object object) schema))
