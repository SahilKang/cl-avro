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

(defpackage #:cl-avro.schema.io
  (:use #:cl-avro.schema.io.read
        #:cl-avro.schema.io.write)
  (:import-from #:cl-avro.schema.io.write.canonicalize
                #:*seen*)
  (:export #:json->schema
           #:parse-schema
           #:*fullname->schema*
           #:make-fullname->schema
           #:*enclosing-namespace*
           #:*error-on-duplicate-name-p*
           #:schema->json
           #:to-jso
           #:*seen*
           #:with-initargs
           #:with-fields
           #:register-named-schema
           #:unregister-named-schema))
