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
(defpackage #:cl-avro.schema.complex.record.notation
  (:use #:cl)
  (:import-from #:cl-avro.schema.primitive
                #:+primitive->name+)
  (:import-from #:cl-avro.schema.complex.fixed
                #:fixed)
  (:import-from #:cl-avro.schema.complex.enum
                #:enum)
  (:shadowing-import-from #:cl-avro.schema.complex.union
                          #:union)
  (:shadowing-import-from #:cl-avro.schema.complex.array
                          #:array)
  (:shadowing-import-from #:cl-avro.schema.complex.map
                          #:map)
  (:export #:parse-notation))
(in-package #:cl-avro.schema.complex.record.notation)

(defgeneric parse-notation (schema notation)

  (:method ((schema fixed) notation)
    (make-instance schema :bytes notation))

  (:method ((schema enum) notation)
    (make-instance schema :enum notation))

  (:method ((schema union) notation)
    (make-instance schema :object notation))

  (:method ((schema array) notation)
    (make-instance schema :initial-contents notation))

  (:method ((schema map) notation)
    (make-instance schema :map notation)))

(macrolet
    ((specialize-parse-notation-for-primitives ()
       (let ((primitives (mapcar #'car +primitive->name+)))
         `(progn
            ,@(mapcar (lambda (schema)
                        `(defmethod parse-notation
                             ((schema (eql ',schema)) notation)
                           (declare (ignore schema))
                           (check-type notation ,schema)
                           notation))
                      primitives)))))
  (specialize-parse-notation-for-primitives))
