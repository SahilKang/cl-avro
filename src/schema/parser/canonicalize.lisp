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

(defgeneric canonicalize (schema)
  (:method :before ((schema named-schema))
    (declare (special *namespace*)
             (optimize (speed 3) (safety 0)))
    (let ((fullname (fullname schema *namespace*)))
      (declare (special *schema->name*))
      (setf (gethash schema *schema->name*) fullname)))

  ;; if schema's already been seen, then just keep the current one and
  ;; let %schema->jso replace it with its name.
  (:method :around (schema)
    (declare (special *schema->name*)
             (optimize (speed 3) (safety 0)))
    (if (nth-value 1 (gethash schema *schema->name*))
        schema
        (call-next-method))))

(macrolet
    ((defprimitives ()
       (flet ((make-defmethod (schema)
                `(defmethod canonicalize ((schema (eql ',schema)))
                   (declare (ignore schema)
                            (optimize (speed 3) (safety 0)))
                   ',schema)))
         `(progn
            ,@(mapcar #'make-defmethod *primitives*))))
     (defaliases ()
       (flet ((make-defmethod (cons)
                (destructuring-bind (logical . underlying)
                    cons
                  `(defmethod canonicalize ((schema (eql ',logical)))
                     (declare (optimize (speed 3) (safety 0)))
                     (canonicalize ',underlying)))))
         `(progn
            ,@(mapcar #'make-defmethod *logical-aliases*)))))
  (defprimitives)
  (defaliases))

(defmethod canonicalize ((schema fixed-schema))
  (declare (special *namespace*)
           (optimize (speed 3) (safety 0)))
  (let ((fullname (fullname schema *namespace*))
        (size (fixed-schema-size schema)))
    (%make-fixed-schema :name fullname :size size)))

(defmethod canonicalize ((schema union-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((schemas (union-schema-schemas schema)))
    (%make-union-schema :schemas (map 'simple-vector #'canonicalize schemas))))

(defmethod canonicalize ((schema array-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((items (array-schema-items schema)))
    (%make-array-schema :items (canonicalize items))))

(defmethod canonicalize ((schema map-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((values (map-schema-values schema)))
    (%make-map-schema :values (canonicalize values))))

(defmethod canonicalize ((schema enum-schema))
  (declare (special *namespace*)
           (optimize (speed 3) (safety 0)))
  (let ((fullname (fullname schema *namespace*))
        (symbols (enum-schema-symbols schema)))
    (%make-enum-schema :name fullname :symbols symbols)))

(defmethod canonicalize ((schema record-schema))
  (declare (special *namespace*)
           (optimize (speed 3) (safety 0)))
  (let ((fullname (fullname schema *namespace*))
        (fields (record-schema-fields schema))
        (namespace (deduce-namespace (record-schema-name schema)
                                     (record-schema-namespace schema)
                                     *namespace*)))
    (%make-record-schema
     :name fullname
     :fields (let ((*namespace* namespace))
               (declare (special *namespace*))
               (map 'simple-vector #'canonicalize fields)))))

(defmethod canonicalize ((schema field-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((name (field-schema-name schema))
        (type (field-schema-type schema)))
    (%make-field-schema :name name :type (canonicalize type))))

(defmethod canonicalize ((schema decimal-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((scale (decimal-schema-scale schema))
        (precision (decimal-schema-precision schema))
        (underlying-schema (decimal-schema-underlying-schema schema)))
    (%make-decimal-schema :scale scale
                          :precision precision
                          :underlying-schema (canonicalize underlying-schema))))

(defmethod canonicalize ((schema duration-schema))
  (declare (optimize (speed 3) (safety 0)))
  (let ((underlying-schema (duration-schema-underlying-schema schema)))
    (%make-duration-schema :underlying-schema (canonicalize underlying-schema))))
