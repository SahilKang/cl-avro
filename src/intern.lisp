;;; Copyright 2023-2024 Google LLC
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
(defpackage #:cl-avro.internal.intern
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro))
  (:export #:*intern-package*))
(in-package #:cl-avro.internal.intern)

(declaim (simple-string api:*null-namespace*))
(defparameter api:*null-namespace* "AVRO.NULL"
  "The default null namespace to use for INTERN.")

(declaim (package *intern-package*))
(defvar *intern-package*)
