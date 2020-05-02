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

(defgeneric canonicalize (schema))


(defmethod canonicalize :before ((schema named-type))
  (declare (special *namespace* *schema->name*))
  (let ((name (deduce-fullname (name schema) (namespace schema) *namespace*)))
    (setf (gethash schema *schema->name*) name)))

(defmethod canonicalize :around (schema)
  (declare (special *schema->name*))
  (let ((*namespace* (when (boundp '*namespace*)
                       (symbol-value '*namespace*))))
    (declare (special *namespace*))
    ;; if schema's already been seen, then just keep the current one
    ;; and let %write-schema replace it with its name.
    (if (nth-value 1 (gethash schema *schema->name*))
        schema
        (call-next-method))))

;; specialize canonicalize methods for primitive avro types:
(defmethods-for-primitives canonicalize nil (schema)
  schema)

(defmethod canonicalize ((schema fixed-schema))
  (declare (special *namespace*))
  (make-instance 'fixed-schema
                 :name (deduce-fullname (name schema)
                                        (namespace schema)
                                        *namespace*)
                 :size (size schema)))

(defmethod canonicalize ((schema union-schema))
  (make-instance 'union-schema
                 :schemas (map 'vector #'canonicalize (schemas schema))))

(defmethod canonicalize ((schema array-schema))
  (make-instance 'array-schema
                 :item-schema (canonicalize (item-schema schema))))

(defmethod canonicalize ((schema map-schema))
  (make-instance 'map-schema
                 :value-schema (canonicalize (value-schema schema))))

(defmethod canonicalize ((schema enum-schema))
  (declare (special *namespace*))
  (make-instance 'enum-schema
                 :name (deduce-fullname (name schema)
                                        (namespace schema)
                                        *namespace*)
                 :symbols (symbols schema)))

(defmethod canonicalize ((schema record-schema))
  (declare (special *namespace*))
  (let* ((name (deduce-fullname (name schema) (namespace schema) *namespace*))
         (namespace (deduce-namespace name nil *namespace*)))
    (make-instance
     'record-schema
     :name name
     :field-schemas (let ((*namespace* namespace))
                      (declare (special *namespace*))
                      (map 'vector
                           (lambda (field-schema)
                             (canonicalize field-schema))
                           (field-schemas schema))))))

(defmethod canonicalize ((schema field-schema))
  (make-instance
   'field-schema
   :name (name schema)
   :field-type (canonicalize (field-type schema))))

(defmethod canonicalize ((schema decimal-schema))
  (make-instance
   'decimal-schema
   :scale (scale schema)
   :precision (precision schema)
   :underlying-schema (canonicalize (underlying-schema schema))))

(defmethod canonicalize ((schema (eql 'uuid-schema)))
  (canonicalize 'string-schema))

(defmethod canonicalize ((schema (eql 'date-schema)))
  (canonicalize 'int-schema))

(defmethod canonicalize ((schema (eql 'time-millis-schema)))
  (canonicalize 'int-schema))

(defmethod canonicalize ((schema (eql 'time-micros-schema)))
  (canonicalize 'long-schema))

(defmethod canonicalize ((schema (eql 'timestamp-millis-schema)))
  (canonicalize 'long-schema))

(defmethod canonicalize ((schema (eql 'timestamp-micros-schema)))
  (canonicalize 'long-schema))
