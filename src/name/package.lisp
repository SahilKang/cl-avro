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

(defpackage #:cl-avro.internal.name
  (:import-from #:cl-avro.internal.name.type
                #:name
                #:namespace
                #:fullname)
  (:import-from #:cl-avro.internal.name.deduce
                #:fullname->name
                #:deduce-namespace
                #:deduce-fullname)
  (:import-from #:cl-avro.internal.name.class
                #:named-class)
  (:import-from #:cl-avro.internal.name.schema
                #:named-schema
                #:valid-name
                #:valid-fullname)
  (:import-from #:cl-avro.internal.name.coerce
                #:assert-matching-names)
  (:export #:name
           #:namespace
           #:fullname
           #:valid-name
           #:valid-fullname
           #:fullname->name
           #:deduce-namespace
           #:deduce-fullname
           #:named-class
           #:named-schema
           #:assert-matching-names))
