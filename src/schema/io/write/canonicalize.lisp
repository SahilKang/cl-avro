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
(defpackage #:cl-avro.schema.io.write.canonicalize
  (:use #:cl)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.logical
                #:logical-schema
                #:underlying

                #:decimal
                #:scale
                #:precision

                #:duration)
  (:import-from #:cl-avro.schema.complex
                #:schema
                #:named-schema
                #:name
                #:fullname

                #:fixed
                #:size

                #:schemas

                #:items

                #:enum
                #:symbols

                #:record
                #:fields)
  (:shadowing-import-from #:cl-avro.schema.complex
                          #:union
                          #:array
                          #:map
                          #:values
                          #:type)
  (:export #:canonicalize
           #:*seen*))
(in-package #:cl-avro.schema.io.write.canonicalize)

(defvar *seen*)

(defgeneric canonicalize (schema)
  (:method :around ((schema named-schema))
    ;; only named schemas can be recursive
    (multiple-value-bind (seen-schema seen-schema-p)
        (gethash schema *seen*)
      (if seen-schema-p
          seen-schema
          ;; this first setf prevents infinite recursion
          (setf (gethash schema *seen*) schema
                (gethash schema *seen*) (call-next-method))))))

(macrolet
    ((defprimitives ()
       (let ((primitives (mapcar #'car +primitive->name+)))
         (flet ((make-defmethod (schema)
                  `(defmethod canonicalize ((schema (eql ',schema)))
                     (declare (ignore schema))
                     ',schema)))
           `(progn
              ,@(mapcar #'make-defmethod primitives))))))
  (defprimitives))

(defmethod canonicalize ((schema fixed))
  (let ((fullname (fullname schema))
        (size (size schema)))
    (make-instance 'fixed :name fullname :size size)))

(defmethod canonicalize ((schema union))
  (let ((schemas (cl:map '(simple-array schema (*))
                         #'canonicalize
                         (schemas schema))))
    (make-instance 'union :schemas schemas)))

(defmethod canonicalize ((schema array))
  (let ((items (canonicalize (items schema))))
    (make-instance 'array :items items)))

(defmethod canonicalize ((schema map))
  (let ((values (canonicalize (values schema))))
    (make-instance 'map :values values)))

(defmethod canonicalize ((schema enum))
  (let ((fullname (fullname schema))
        (symbols (symbols schema)))
    (make-instance 'enum :name fullname :symbols symbols)))

(defmethod canonicalize ((schema record))
  (let ((fullname (fullname schema))
        (fields (cl:map 'list
                        (lambda (field)
                          (let ((name (nth-value 1 (name field)))
                                (type (canonicalize (type field))))
                            (list :name name :type type)))
                        (fields schema))))
    (make-instance 'record :name fullname :direct-slots fields)))

;; TODO this just returns the underlying...that's probably not intended
(defmethod canonicalize ((schema logical-schema))
  (canonicalize (underlying schema)))

(defmethod canonicalize :around ((schema decimal))
  (let ((initargs (list
                   :precision (precision schema)
                   :underlying (call-next-method))))
    (multiple-value-bind (scale scalep) (scale schema)
      (when scalep
        (push scale initargs)
        (push :scale initargs)))
    (apply #'make-instance 'decimal initargs)))

(defmethod canonicalize :around ((schema duration))
  (make-instance 'duration :underlying (call-next-method)))
