;;; Copyright 2021 Google LLC
;;; Copyright 2025 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(cl:in-package #:cl-user)
(defpackage #:cl-avro.internal.name.coerce
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:deduce #:cl-avro.internal.name.deduce)
   (#:schema #:cl-avro.internal.name.schema))
  (:export #:assert-matching-names))
(in-package #:cl-avro.internal.name.coerce)

(declaim
 (ftype (function (schema:valid-name schema:valid-fullname)
                  (values boolean &optional))
        matching-alias-p))
(defun matching-alias-p (writer-name reader-alias)
  (string= writer-name (deduce:fullname->name reader-alias)))

(declaim
 (ftype (function (schema:valid-name schema:array<alias>)
                  (values boolean &optional))
        matching-aliases-p))
(defun matching-aliases-p (writer-name reader-aliases)
  (flet ((matching-alias-p (reader-alias)
           (matching-alias-p writer-name reader-alias)))
    (some #'matching-alias-p reader-aliases)))

(declaim
 (ftype (function (schema:named-schema schema:named-schema)
                  (values boolean &optional))
        matching-names-p))
(defun matching-names-p (reader writer)
  (let ((reader-name (api:name reader))
        (writer-name (api:name writer)))
    (declare (schema:valid-name reader-name writer-name))
    (or (string= reader-name writer-name)
        (let ((reader-aliases (api:aliases reader)))
          (declare (schema:array<alias>? reader-aliases))
          (when reader-aliases
            (matching-aliases-p writer-name reader-aliases))))))

(declaim
 (ftype (function (schema:named-schema schema:named-schema) (values &optional))
        assert-matching-names))
(defun assert-matching-names (reader writer)
  (unless (matching-names-p reader writer)
    (error "Names don't match between reader schema ~S and writer schema ~S"
           reader writer))
  (values))
