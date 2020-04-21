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

(defun same-name-p (reader-schema writer-schema)
  (declare (special *reader-namespace* *writer-namespace*))
  (let* ((rnamespace (namespace reader-schema))
         (rname (deduce-fullname (name reader-schema)
                                 rnamespace
                                 *reader-namespace*))
         (wname (deduce-fullname (name writer-schema)
                                 (namespace writer-schema)
                                 *writer-namespace*)))
    (if (string= rname wname)
        t
        (loop
           for name across (aliases reader-schema)
           for fullname = (deduce-fullname name rnamespace *reader-namespace*)
           when (string= fullname wname)
           do (return-from same-name-p t)))))


(defgeneric resolve (reader-schema writer-schema)
  (:documentation
   "Return an avro schema that resolves the differences between the inputs.

Resolution is determined by the Schema Resolution rules in the avro spec."))

(defgeneric matchp (reader-schema writer-schema)
  (:documentation
   "Determine if schemas match for schema resolution.")
  (:method (reader-schema writer-schema)
    nil))


(defmethod resolve :before (reader-schema writer-schema)
  (unless (matchp reader-schema writer-schema)
    (error "Schemas don't match")))

(defmethod resolve :around (reader-schema writer-schema)
  (let ((*reader-namespace* (when (boundp '*reader-namespace*)
                              (symbol-value '*reader-namespace*)))
        (*writer-namespace* (when (boundp '*writer-namespace*)
                              (symbol-value '*writer-namespace*))))
    (declare (special *reader-namespace* *writer-namespace*))
    (call-next-method)))

(defmethod deserialize :around (input schema &optional writer-schema)
  (if writer-schema
      (call-next-method input (resolve schema writer-schema))
      (call-next-method)))


(defmethod matchp ((reader-schema array-schema) (writer-schema array-schema))
  (let ((reader-schema (item-schema reader-schema))
        (writer-schema (item-schema writer-schema)))
    (eq (type-of reader-schema) (type-of writer-schema))))

(defmethod resolve ((reader-schema array-schema) (writer-schema array-schema))
  (make-instance 'array-schema
                 :item-schema (resolve reader-schema writer-schema)))


(defmethod matchp ((reader-schema map-schema) (writer-schema map-schema))
  (let ((reader-schema (value-schema reader-schema))
        (writer-schema (value-schema writer-schema)))
    (eq (type-of reader-schema) (type-of writer-schema))))

(defmethod resolve ((reader-schema map-schema) (writer-schema map-schema))
  (make-instance 'map-schema
                 :value-schema (resolve reader-schema writer-schema)))


(defmethod matchp ((reader-schema enum-schema) (writer-schema enum-schema))
  (same-name-p reader-schema writer-schema))

(defclass resolved-enum-schema (enum-schema)
  ((reader-symbols
    :initarg :reader-symbols
    :initform (error "Must supply :reader-symbols")
    :type (typed-vector avro-name)
    :documentation "Enum symbols known to the reader")))

(defmethod deserialize :around ((stream stream)
                                (schema resolved-enum-schema)
                                &optional writer-schema)
  (declare (ignore writer-schema))
  (with-slots (reader-symbols) schema
    (let ((string (call-next-method)))
      (if (position string reader-symbols :test #'string=)
          string
          (nth-value 0 (default schema))))))

(defmethod resolve ((reader-schema enum-schema) (writer-schema enum-schema))
  ;; from the spec:
  ;; if the writer's symbol is not present in the reader's enum and
  ;; the reader has a default value, then that value is used,
  ;; otherwise an error is signalled.
  (unless (set-difference (coerce (symbols writer-schema) 'list)
                          (coerce (symbols reader-schema) 'list)
                          :test #'string=)
    (return-from resolve reader-schema))

  (unless (default reader-schema)
    (error "~&Reader enum has no default value for unknown writer symbols"))

  (make-instance 'resolved-enum-schema
                 :name (name reader-schema)
                 :default (default reader-schema)
                 :symbols (symbols writer-schema)
                 :reader-symbols (symbols reader-schema)))


(defmethod matchp ((reader-schema fixed-schema) (writer-schema fixed-schema))
  (let ((rsize (size reader-schema))
        (wsize (size writer-schema)))
    (and (same-name-p reader-schema writer-schema) (= rsize wsize))))

(defmethod resolve ((reader-schema fixed-schema) (writer-schema fixed-schema))
  reader-schema)


(defmethod matchp ((reader-schema record-schema) (writer-schema record-schema))
  (same-name-p reader-schema writer-schema))

(defclass resolved-record-schema (record-schema)
  ((windex->rindex
    :initarg :windex->rindex
    :initform (error "Must supply :windex->rindex hash-table")
    :type hash-table
    :documentation "Maps the writer's index to the reader's index.")
   (rindex->default
    :initarg :rindex->default
    :initform (error "Must supply :rindex->default hash-table")
    :type hash-table
    :documentation
    "Indicates which indices should be filled with reader defaults.")))

(defmethod deserialize :around ((stream stream)
                                (schema resolved-record-schema)
                                &optional writer-schema)
  (declare (ignore writer-schema))
  (with-slots (windex->rindex rindex->default) schema
    (let ((rfields (make-array (+ (hash-table-count windex->rindex)
                                  (hash-table-count rindex->default))))
          (wfields (call-next-method)))
      (maphash (lambda (windex rindex)
                 (setf (elt rfields rindex) (elt wfields windex)))
               windex->rindex)
      (maphash (lambda (rindex default)
                 (setf (elt rfields rindex) default))
               rindex->default)
      rfields)))

(defmethod resolve ((reader-schema record-schema) (writer-schema record-schema))
  (declare (special *reader-namespace* *writer-namespace*))
  (let ((windex->rindex (make-hash-table :test #'eql))
        (rindex->default (make-hash-table :test #'eql))
        (new-fields (copy-seq (field-schemas writer-schema)))
        (*reader-namespace* (deduce-namespace (name reader-schema)
                                              (namespace reader-schema)
                                              *reader-namespace*))
        (*writer-namespace* (deduce-namespace (name writer-schema)
                                              (namespace writer-schema)
                                              *writer-namespace*)))
    (declare (special *reader-namespace* *writer-namespace*))
    ;; from the spec:
    ;; * the ordering of fields may be different: fields are matched by name.
    ;; * schemas for fields with the same name in both records are resolved
    ;;   recursively.
    ;; * if the writer's record contains a field with a name not present in
    ;;   the reader's record, the writer's value for that field is ignored.
    ;; * if the reader's record schema has a field that contains a default
    ;;   value, and writer's schema does not have a field with the same name,
    ;;   then the reader should use the default value from its field.
    ;; * if the reader's record schema has a field with no default value,
    ;;   and writer's schema does not have a field with the same name,
    ;;   an error is signalled.
    (loop
       with wfields = (field-schemas writer-schema)
       with rfields = (field-schemas reader-schema)

       for wfield across wfields and windex from 0
       for rindex = (position-if (lambda (rfield)
                                   (same-name-p rfield wfield))
                                 rfields)

       when rindex
       do (let* ((rfield (elt rfields rindex))
                 (*reader-namespace* (deduce-namespace (name rfield)
                                                       nil
                                                       *reader-namespace*))
                 (*writer-namespace* (deduce-namespace (name wfield)
                                                       nil
                                                       *writer-namespace*)))
            (declare (special *reader-namespace* *writer-namespace*))
            (setf (gethash windex windex->rindex) rindex
                  (elt new-fields windex) (make-instance
                                           'field-schema
                                           :name (name rfield)
                                           :field-type (resolve
                                                        (field-type rfield)
                                                        (field-type wfield))))))
    (loop
       with rfields = (field-schemas reader-schema)
       with accounted-for = (loop
                               for v being the hash-values of windex->rindex
                               collect v)
       with unaccounted-for = (set-difference (loop
                                                 for i below (length rfields)
                                                 collect i)
                                              accounted-for)

       for rindex in unaccounted-for
       for rfield = (elt rfields rindex)
       for (default defaultp) = (multiple-value-list (default rfield))

       if defaultp
       do (setf (gethash rindex rindex->default) default)
       else do (error "~&No default reader field for unknown writer field."))
    (make-instance 'resolved-record-schema
                   :name (name reader-schema)
                   :field-schemas new-fields
                   :windex->rindex windex->rindex
                   :rindex->default rindex->default)))


(defmethod matchp ((reader-schema union-schema) writer-schema)
  t)

(defmethod matchp (reader-schema (writer-schema union-schema))
  t)

(defmethod resolve ((reader-schema union-schema) (writer-schema union-schema))
  (loop
     with wschemas = (schemas writer-schema)

     for rschema across (schemas reader-schema)
     for match = (find-if (lambda (x) (matchp rschema x)) wschemas)
     when match
     do (return-from resolve (resolve rschema match)))
  (error "~&Union schemas don't match"))

(defmethod resolve ((reader-schema union-schema) writer-schema)
  (loop
     for rschema across (schemas reader-schema)
     when (matchp rschema writer-schema)
     do (return-from resolve (resolve rschema writer-schema)))
  (error "~&Union reader schema does not match writer-schema."))

(defmethod resolve (reader-schema (writer-schema union-schema))
  (resolve writer-schema reader-schema))


(defmethod matchp ((reader-schema decimal-schema) (writer-schema decimal-schema))
  (and (= (scale reader-schema) (scale writer-schema))
       (= (precision reader-schema) (precision writer-schema))))

(defmethod resolve ((reader-schema decimal-schema) (writer-schema decimal-schema))
  reader-schema)


;; specialize matchp and resolve methods for primitive avro types:

(defmethods-for-primitives matchp nil (reader-schema writer-schema)
  t)

(defmethods-for-primitives resolve nil (reader-schema writer-schema)
  reader-schema)


;; TODO need to coerce the output type appropriately
;; should move bytes/string outside of this macrolet since they're different

;; specialize matchp and resolve methods to promote writer's schema
(macrolet
    ((promote ((&rest readers) writer)
       (let* ((->schema (lambda (symbol)
                          (read-from-string (format nil "~A-schema" symbol))))
              (reader-schemas (loop
                                 for symbol in readers
                                 for schema = (find-if
                                               (lambda (s)
                                                 (eq s (funcall ->schema symbol)))
                                               +primitive-schemas+)
                                 when schema
                                 collect schema))
              (writer-schema (loop
                                with writer-schema = (funcall ->schema writer)
                                for symbol in +primitive-schemas+
                                when (eq symbol writer-schema)
                                return writer-schema)))
         (unless (= (length reader-schemas) (length readers))
           (error "~&Not all reader-schemas were found"))
         (unless writer-schema
           (error "~&writer-schema was not found"))
         `(progn
            ,@(loop
                 for reader-schema in reader-schemas
                 collect `(defmethod matchp
                              ((reader-schema (eql ',reader-schema))
                               (writer-schema (eql ',writer-schema)))
                            t)
                 collect `(defmethod resolve
                              ((reader-schema (eql ',reader-schema))
                               (writer-schema (eql ',writer-schema)))
                            writer-schema))))))
  ;; from the spec:
  ;; the writer's schema may be promoted to the reader's as follows:
  ;;   * int is promotable to long, float, or double
  ;;   * long is promotable to float or double
  ;;   * float is promotable to double
  ;;   * string is promotable to bytes
  ;;   * bytes is promotable to string
  (promote (long float double) int)
  (promote (float double) long)
  (promote (double) float)
  (promote (bytes) string)
  (promote (string) bytes))
