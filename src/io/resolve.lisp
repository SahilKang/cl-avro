;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(in-package #:cl-avro)

;; TODO break out into separate files

;;; assert-match

(defgeneric assert-match (reader writer)
  (:method (reader writer)
    (declare (optimize (speed 3) (safety 0)))
    (error "Reader schema ~S does not match writer schema ~S" reader writer))

  (:documentation
   "Asserts that schemas match for schema resolution."))

(defun! matching-names-p (reader writer)
    ((named-schema named-schema) boolean)
  (declare (inline unqualified-name))
  (let ((reader-name (unqualified-name (named-schema-name reader)))
        (writer-name (unqualified-name (named-schema-name writer))))
    (or (string= reader-name writer-name)
        (some (lambda (reader-alias)
                (string= (unqualified-name reader-alias) writer-name))
              (named-schema-aliases reader)))))

(defun! assert-matching-names (reader writer)
    ((named-schema named-schema) (values))
  (declare (inline matching-names-p))
  (unless (matching-names-p reader writer)
    (error "Names don't match between reader schema ~S and writer schema ~S"
           reader
           writer)))

;; avro primitive schemas

(macrolet
    ((defprimitives ()
       (flet ((make-defmethod (schema)
                `(defmethod assert-match ((reader (eql ',schema))
                                          (writer (eql ',schema)))
                   (declare (ignore reader writer)
                            (optimize (speed 3) (safety 0))))))
         `(progn
            ,@(mapcar #'make-defmethod *primitives*)))))
  (defprimitives))

;; array-schema

(defmethod assert-match ((reader array-schema)
                         (writer array-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((reader-items (array-schema-items reader))
        (writer-items (array-schema-items writer)))
    (assert-match reader-items writer-items)))

;; map-schema

(defmethod assert-match ((reader map-schema)
                         (writer map-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((reader-values (map-schema-values reader))
        (writer-values (map-schema-values writer)))
    (assert-match reader-values writer-values)))

;; enum-schema

(defmethod assert-match ((reader enum-schema)
                         (writer enum-schema))
  (declare (inline assert-matching-names)
           (optimize (speed 3) (safety 0)))
  (assert-matching-names reader writer))

;; fixed-schema

(defmethod assert-match ((reader fixed-schema)
                         (writer fixed-schema))
  (declare (inline assert-matching-names)
           (optimize (speed 3) (safety 0)))
  (let ((reader-size (fixed-schema-size reader))
        (writer-size (fixed-schema-size writer)))
    (unless (= reader-size writer-size)
      (error "Reader and writer fixed schemas have different sizes: ~S and ~S"
             reader-size
             writer-size)))
  (assert-matching-names reader writer))

;; record-schema

(defmethod assert-match ((reader record-schema)
                         (writer record-schema))
  (declare (inline assert-matching-names)
           (optimize (speed 3) (safety 0)))
  (assert-matching-names reader writer))

;; union-schema

(defmethod assert-match ((reader union-schema)
                         writer)
  (declare (ignore reader writer)
           (optimize (speed 3) (safety 0))))

(defmethod assert-match (reader
                         (writer union-schema))
  (declare (ignore reader writer)
           (optimize (speed 3) (safety 0))))

;; logical schemas

(macrolet
    ((defaliases ()
       (flet ((make-defmethod (cons)
                (destructuring-bind (logical . underlying)
                    cons
                  `((defmethod assert-match ((reader (eql ',logical))
                                             writer)
                      (declare (ignore reader)
                               (optimize (speed 3) (safety 0)))
                      (assert-match ',underlying writer))
                    (defmethod assert-match (reader
                                             (writer (eql ',logical)))
                      (declare (ignore writer)
                               (optimize (speed 3) (safety 0)))
                      (assert-match reader ',underlying))))))
         `(progn
            ,@(mapcan #'make-defmethod *logical-aliases*)))))

  (defaliases)

  (defmethod assert-match ((reader logical-schema)
                           writer)
    (declare (optimize (speed 3) (safety 0)))
    (assert-match (logical-schema-underlying-schema reader) writer))

  (defmethod assert-match (reader
                           (writer logical-schema))
    (declare (optimize (speed 3) (safety 0)))
    (assert-match reader (logical-schema-underlying-schema writer))))

;; decimal-schema

(defmethod assert-match ((reader decimal-schema)
                         (writer decimal-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((reader-scale (decimal-schema-scale reader))
        (reader-precision (decimal-schema-precision reader))
        (writer-scale (decimal-schema-scale writer))
        (writer-precision (decimal-schema-precision writer)))
    (unless (= reader-scale writer-scale)
      (error "Reader and writer's decimal scales don't match: ~S and ~S"
             reader-scale
             writer-scale))
    (unless (= reader-precision writer-precision)
      (error "Reader and writer's decimal precisions don't match: ~S and ~S"
             reader-precision
             writer-precision))))

;; duration-schema

;; TODO make sure this is okay (this ignores the name of the
;; underlying fixed schema)
(defmethod assert-match ((reader duration-schema)
                         (writer duration-schema))
  (declare (ignore reader writer)
           (optimize (speed 3) (safety 0))))

;;; resolve

(defgeneric resolve (reader writer)
  (:method :before (reader writer)
    (declare (optimize (speed 3) (safety 0)))
    (assert-match reader writer))

  (:method resolve (reader writer)
    (declare (ignore writer)
             (optimize (speed 3) (safety 0)))
    reader)

  (:documentation
   "Return a schema that resolves the differences between the inputs."))

(defmethod deserialize :around (reader-schema input &optional writer-schema)
  (declare (optimize (speed 3) (safety 0)))
  (if writer-schema
      (call-next-method (resolve reader-schema writer-schema) input)
      (call-next-method)))

;; array-schema

(defmethod resolve ((reader array-schema)
                    (writer array-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((reader-items (array-schema-items reader))
        (writer-items (array-schema-items writer)))
    (%make-array-schema :items (resolve reader-items writer-items))))

;; map-schema

(defmethod resolve ((reader map-schema)
                    (writer map-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((reader-values (map-schema-values reader))
        (writer-values (map-schema-values writer)))
    (%make-map-schema :values (resolve reader-values writer-values))))

;; enum-schema

(defstruct (resolved-enum-schema
            (:include enum-schema
             (default (error "Must supply DEFAULT") :type avro-name :read-only t))
            (:copier nil)
            (:predicate nil))
  (known-symbols (error "Must supply KNOWN-SYMBOLS") :type hash-table :read-only t))

(defun! get-known-symbols (reader)
    ((enum-schema) hash-table)
  (loop
    with symbols = (enum-schema-symbols reader)
    with known-symbols = (make-hash-table :test #'equal :size (length symbols))

    for symbol across symbols
    do (setf (gethash symbol known-symbols) t)

    finally (return known-symbols)))

(defun! all-known-symbols-p (known-symbols writer)
    ((hash-table enum-schema) boolean)
  (every (lambda (writer-symbol)
           (gethash writer-symbol known-symbols))
         (enum-schema-symbols writer)))

(defmethod resolve ((reader enum-schema)
                    (writer enum-schema))
  (declare (inline get-known-symbols all-known-symbols-p)
           (optimize (speed 3) (safety 0)))
  (let ((known-symbols (get-known-symbols reader)))
    (if (all-known-symbols-p known-symbols writer)
        reader
        (let ((default (enum-schema-default reader)))
          (unless default
            (error "Reader enum has no default for unknown writer symbols"))
          (make-resolved-enum-schema
           :name (enum-schema-name reader)
           :symbols (enum-schema-symbols writer)
           :known-symbols known-symbols
           :default default)))))

(defmethod deserialize ((schema resolved-enum-schema)
                        (stream stream)
                        &optional writer-schema)
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let ((known-symbols (resolved-enum-schema-known-symbols schema))
        (default (resolved-enum-schema-default schema))
        (symbol (call-next-method)))
    (if (gethash symbol known-symbols)
        symbol
        default)))

;; record-schema

(defvec avro-name)

(defstruct (resolved-record-schema
            (:include record-schema)
            (:copier nil)
            (:predicate nil))
  (unknown-fields (error "Must supply UNKNOWN-FIELDS") :type vector[avro-name] :read-only t)
  (field->default (error "Must supply FIELD->DEFAULT") :type hash-table :read-only t))

(defun! make-name->field (schema)
    ((record-schema) hash-table)
  (loop
    with fields = (record-schema-fields schema)
    with name->field = (make-hash-table :test #'equal :size (length fields))

    for field across fields
    for name = (field-schema-name field)
    do (setf (gethash name name->field) field)

    finally (return name->field)))

(defun! make-field->default (name->reader-field writer)
    ((hash-table record-schema) hash-table)
  (declare (inline make-name->field))
  (let ((field->default (make-hash-table :test #'equal))
        (name->writer-field (make-name->field writer)))
    (maphash (lambda (name reader-field)
               (unless (gethash name name->writer-field)
                 (unless (field-schema-defaultp reader-field)
                   (error "Writer field ~S does not exist and reader has no default" name))
                 (let ((default (field-schema-default reader-field)))
                   (setf (gethash name field->default) default))))
             name->reader-field)
    field->default))

(defmethod resolve ((reader record-schema)
                    (writer record-schema))
  (declare (inline make-name->field make-field->default)
           (optimize (speed 3) (safety 0)))
  (loop
    with name->reader-field = (make-name->field reader)
    and writer-fields = (record-schema-fields writer)
    with field->default = (make-field->default name->reader-field writer)
    and resolved-fields = (make-array (length writer-fields))
    and unknown-fields = (make-array 0 :element-type 'avro-name :adjustable t :fill-pointer 0)

    for writer-field across writer-fields
    for i = 0 then (1+ i)
    for name = (field-schema-name writer-field)
    for reader-field = (gethash name name->reader-field)

    if reader-field do
      (let* ((reader-type (field-schema-type reader-field))
             (writer-type (field-schema-type writer-field))
             (resolved-field (%make-field-schema
                              :name name
                              :type (resolve reader-type writer-type))))
        (setf (svref resolved-fields i) resolved-field))

    else do
      (setf (svref resolved-fields i) writer-field)
      (vector-push-extend name unknown-fields)

    finally
       (return (make-resolved-record-schema
                :name (record-schema-name reader)
                :fields resolved-fields
                :field->default field->default
                :unknown-fields (coerce unknown-fields 'simple-vector)))))

(defmethod deserialize ((schema resolved-record-schema)
                        (stream stream)
                        &optional writer-schema)
  (declare (ignore writer-schema)
           (optimize (speed 3) (safety 0)))
  (let ((unknown-fields (resolved-record-schema-unknown-fields schema))
        (field->default (resolved-record-schema-field->default schema))
        (record (call-next-method)))
    (flet ((remove-field (field)
             (declare (avro-name field)
                      (optimize (speed 3) (safety 0)))
             (remhash field record))
           (put-default (field default)
             (declare (avro-name field)
                      (optimize (speed 3) (safety 0)))
             (setf (gethash field record) default)))
      (map nil #'remove-field unknown-fields)
      (maphash #'put-default field->default))
    record))

;; union-schema

(defstruct (resolved-union-schema
            (:include union-schema)
            (:copier nil)
            (:predicate nil))
  (known-schemas (error "Must supply KNOWN-SCHEMAS") :type vector[avro-schema] :read-only t))

(defmethod resolve ((reader union-schema)
                    (writer union-schema))
  (declare (optimize (speed 3) (safety 0)))
  (make-resolved-union-schema
   :schemas (union-schema-schemas writer)
   :known-schemas (union-schema-schemas reader)))

(defun! matchp (reader writer)
    ((t t) boolean)
  (handler-case
      (progn
        (assert-match reader writer)
        t)
    (error ()
      nil)))

(defmethod resolve ((reader union-schema)
                    writer)
  (declare (inline matchp)
           (optimize (speed 3) (safety 0)))
  (let* ((reader-schemas (union-schema-schemas reader))
         (first-match (find-if (lambda (reader-schema)
                                 (matchp reader-schema writer))
                               reader-schemas)))
    (unless first-match
      (error "None of the reader union's schemas match the writer schema"))
    (resolve first-match writer)))

(defmethod resolve (reader
                    (writer union-schema))
  (declare (optimize (speed 3) (safety 0)))
  (make-resolved-union-schema
   :schemas (union-schema-schemas writer)
   :known-schemas (vector reader)))

(defmethod deserialize ((schema resolved-union-schema)
                        (stream stream)
                        &optional writer-schema)
  (declare (ignore writer-schema)
           (inline matchp)
           (optimize (speed 3) (safety 0)))
  (let* ((writer-schema (svref (resolved-union-schema-schemas schema)
                               (deserialize 'long-schema stream)))
         (reader-schemas (resolved-union-schema-known-schemas schema))
         (first-match (find-if (lambda (reader-schema)
                                 (matchp reader-schema writer-schema))
                               reader-schemas)))
    (unless first-match
      (deserialize writer-schema stream) ; consume stream
      (error "None of the reader's schemas match the writer's"))
    (let ((resolved-schema (resolve first-match writer-schema)))
      (deserialize resolved-schema stream))))

;; promoted schemas

(defunc schema-suffix-p (name)
  (declare (simple-string name))
  (let ((last-hyphen-position (position #\- name :test #'char= :from-end t)))
    (when last-hyphen-position
      (string= "-SCHEMA" (subseq name last-hyphen-position)))))

(defunc schema-name (schema-symbol)
  (declare (symbol schema-symbol))
  (let ((name (symbol-name schema-symbol)))
    (if (schema-suffix-p name)
        name
        (format nil "~A-SCHEMA" name))))

(defunc find-schema (schema-symbol)
  (declare (symbol schema-symbol))
  (let ((schema-name (schema-name schema-symbol)))
    (multiple-value-bind (schema status)
        (find-symbol schema-name)
      (unless (eq status :external)
        (error "~S does not name an external symbol" schema-name))
      schema)))

(defunc to-method-specifier (schema)
  (declare (symbol schema))
  (if (typep schema 'primitive-schema)
      `(eql ',schema)
      schema))

(defmacro defpromoted ((from to) &body body)
  (declare (symbol from to))
  (let ((promoted-symbol (intern (format nil "~S->~S" from to)))
        (from (to-method-specifier (find-schema from)))
        (to (to-method-specifier (find-schema to))))
    `(progn
       (defmethod assert-match ((reader ,to)
                                (writer ,from))
         (declare (ignore reader writer)
                  (optimize (speed 3) (safety 0))))

       (defmethod resolve ((reader ,to)
                           (writer ,from))
         (declare (ignore reader writer)
                  (optimize (speed 3) (safety 0)))
         ',promoted-symbol)

       (defmethod deserialize ((schema (eql ',promoted-symbol))
                               (stream stream)
                               &optional writer-schema)
         (declare (ignore writer-schema)
                  (optimize (speed 3) (safety 0)))
         ,@body))))

(defmacro promote (from (&rest tos))
  (declare (symbol from))
  (setf from (find-schema from)
        tos (mapcar #'find-schema tos))
  (flet ((make-defpromoted (to)
           (declare (symbol to))
           (let ((deserialized (gensym)))
             `(defpromoted (,from ,to)
                (let ((,deserialized (deserialize ',from stream)))
                  (declare (,from ,deserialized))
                  (coerce ,deserialized ',to))))))
    `(progn
       ,@(mapcar #'make-defpromoted tos))))

(promote int (long float double))

(promote long (float double))

(promote float (double))

(defpromoted (string bytes)
  (deserialize 'bytes-schema stream))

(defpromoted (bytes string)
  (deserialize 'string-schema stream))
