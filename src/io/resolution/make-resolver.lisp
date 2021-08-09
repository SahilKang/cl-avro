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
(defpackage #:cl-avro.io.resolution.make-resolver
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:make-resolver)
  (:export #:make-resolver))
(in-package #:cl-avro.io.resolution.make-resolver)

;;; fixed schema

(defmethod make-resolver
    ((reader schema:fixed) (writer schema:fixed))
  (lambda (writer-fixed)
    (make-instance reader :initial-contents writer-fixed)))

;;; array schema

(defmethod make-resolver
    ((reader schema:array) (writer schema:array))
  (let* ((reader-items (schema:items reader))
         (writer-items (schema:items writer))
         (resolve-item (make-resolver reader-items writer-items)))
    (lambda (writer-array)
      (make-instance
       reader
       :initial-contents (map `(simple-array ,reader-items (*))
                              resolve-item
                              writer-array)))))

;;; map schema

(defmethod make-resolver
    ((reader schema:map) (writer schema:map))
  (let* ((reader-values (schema:values reader))
         (writer-values (schema:values writer))
         (resolve-value (make-resolver reader-values  writer-values)))
    (lambda (writer-map)
      (let ((reader-map (make-instance
                         reader
                         :size (schema:generic-hash-table-size writer-map))))
        (flet ((fill-reader-map (key value)
                 (setf (schema:hashref key reader-map)
                       (funcall resolve-value value))))
          (schema:hashmap #'fill-reader-map writer-map))
        reader-map))))

;;; enum schema

(defmethod make-resolver
    ((reader schema:enum) (writer schema:enum))
  (let ((reader-default (schema:default reader)))
    (lambda (writer-enum)
      (let ((writer-symbol (schema:which-one writer-enum)))
        (handler-case
            (make-instance reader :enum writer-symbol)
          (error ()
            (if reader-default
                (make-instance reader :enum reader-default)
                (error "Reader enum has no default for unknown writer symbol ~S"
                       writer-symbol))))))))

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

(defmethod make-resolver
    ((reader schema:record) (writer schema:record))
  (loop
    with initargs = nil
    and to-process = nil
    and writer-fields = (schema:fields writer)

    for reader-field across (schema:fields reader)
    for initarg = (intern (schema:name reader-field) 'keyword)
    for writer-field = (find-field reader-field writer-fields)

    if writer-field do
      (let* ((reader-type (schema:type reader-field))
             (writer-type (schema:type writer-field))
             (resolve-field (make-resolver reader-type writer-type))
             (writer-slot (nth-value 1 (schema:name writer-field)))
             (lambda (lambda (writer-record)
                       (let ((field (slot-value writer-record writer-slot)))
                         (funcall resolve-field field)))))
        (push (cons initarg lambda) to-process))
    else do
      (multiple-value-bind (default defaultp)
          (schema:default reader-field)
        (unless defaultp
          (error "Writer field ~S does not exist and reader has no default"
                 (schema:name reader-field)))
        (push default initargs)
        (push initarg initargs))

    finally
       (return
         (lambda (writer-record)
           (loop
             for (initarg . resolve-field) in to-process
             do
                (push (funcall resolve-field writer-record) initargs)
                (push initarg initargs)
             finally
                (return
                  (apply #'make-instance reader initargs)))))))

;;; union schema

(declaim
 (ftype (function (schema:schema schema:schema)
                  (values (or null (function (schema:object)
                                             (values schema:object &optional)))
                          &optional))
        make-resolver?))
(defun make-resolver? (reader writer)
  (handler-case
      (make-resolver reader writer)
    (error ()
      nil)))

(declaim
 (ftype (function (schema:schema (simple-array schema:schema (*)))
                  (values (or null (function (schema:object)
                                             (values schema:object &optional)))
                          &optional))
        find-resolver))
(defun find-resolver (writer readers)
  (flet ((make-resolver? (reader)
           (make-resolver? reader writer)))
    (some #'make-resolver? readers)))

(defmethod make-resolver
    ((reader schema:union) (writer schema:union))
  (let ((readers (schema:schemas reader)))
    (lambda (writer-union-object)
      (let* ((chosen-schema (nth-value 2 (schema:which-one writer-union-object)))
             (chosen-object (schema:object writer-union-object))
             (resolve-object (find-resolver chosen-schema readers)))
        (unless resolve-object
          (error "None of the reader union's schemas match the chosen writer schema"))
        (make-instance reader :object (funcall resolve-object chosen-object))))))

(defmethod make-resolver
    ((reader schema:union) writer)
  (let ((resolve-object (find-resolver writer (schema:schemas reader))))
    (unless resolve-object
      (error "None of the reader union's schemas match the writer schema"))
    (lambda (writer-object)
      (make-instance reader :object (funcall resolve-object writer-object)))))

(defmethod make-resolver
    (reader (writer schema:union))
  (lambda (writer-union-object)
    (let* ((chosen-schema (nth-value 2 (schema:which-one writer-union-object)))
           (chosen-object (schema:object writer-union-object))
           (resolve (make-resolver reader chosen-schema)))
      (funcall resolve chosen-object))))
