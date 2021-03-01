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

(in-package #:cl-user)
(defpackage #:cl-avro.io.resolution.promoted
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema))
  (:import-from #:cl-avro.io.base
                #:deserialize)
  (:import-from #:cl-avro.io.resolution.assert-match
                #:assert-match)
  (:import-from #:cl-avro.io.resolution.resolve
                #:resolve
                #:resolved
                #:reader
                #:writer)
  (:export #:deserialize
           #:resolve
           #:assert-match))
(in-package #:cl-avro.io.resolution.promoted)

;; promoted

(eval-when (:compile-toplevel :load-toplevel)
  (defclass promoted (resolved)
    ((reader
      :type schema:primitive-schema)
     (writer
      :type schema:primitive-schema))))

(defmethod deserialize
    ((schema promoted) (stream stream) &key)
  (declare (optimize (speed 3) (safety 0)))
  (let ((value (deserialize (writer schema) stream)))
    (coerce value (reader schema))))

;; promoted-string-or-bytes

(eval-when (:compile-toplevel :load-toplevel)
  (defclass promoted-string-or-bytes (resolved)
    ((reader
      :type (or schema:string schema:bytes))
     (writer
      :type (or schema:string schema:bytes)))))

(defmethod deserialize
    ((schema promoted-string-or-bytes) (stream stream) &key)
  (declare (optimize (speed 3) (safety 0)))
  (deserialize (reader schema) stream))

;; promote

(eval-when (:compile-toplevel)
  (declaim
   (ftype (function (symbol) (values schema:primitive-schema &optional))
          find-schema))
  (defun find-schema (symbol)
    (let ((schema (find-symbol (string symbol) 'schema)))
      (check-type schema schema:primitive-schema)
      schema)))

(defmacro promote (from (&rest tos) &optional (resolved-class 'promoted))
  (unless (subtypep resolved-class 'resolved)
    (error "~S is not a subclass of resolved" resolved-class))
  (let ((from-schema (find-schema from))
        (to-schemas (mapcar #'find-schema tos)))
    (flet ((make-defmethods (to-schema)
             `((defmethod assert-match
                   ((reader (eql ',to-schema)) (writer (eql ',from-schema)))
                 (declare (optimize (speed 3) (safety 0))
                          (ignore reader writer))
                 (values))

               (defmethod resolve
                   ((reader (eql ',to-schema)) (writer (eql ',from-schema)))
                 (declare (optimize (speed 3) (safety 0)))
                 (make-instance ',resolved-class
                                :reader reader :writer writer)))))
      `(progn
         ,@(mapcan #'make-defmethods to-schemas)))))

(promote int (long float double))

(promote long (float double))

(promote float (double))

(promote string (bytes) promoted-string-or-bytes)

(promote bytes (string) promoted-string-or-bytes)
