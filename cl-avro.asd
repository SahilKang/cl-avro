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

(asdf:defsystem #:cl-avro
  :description
  "Implementation of the Apache Avro data serialization system."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:trivial-gray-streams
               #:babel
               #:ieee-floats
               #:st-json
               #:chipz
               #:salza2)
  :in-order-to ((test-op (test-op #:cl-avro/test)))
  :build-pathname "cl-avro"
  :pathname "src"
  :components
  ((:file "package")
   (:file "common" :depends-on ("package"))
   (:module "schema"
            :depends-on ("common")
            :components
            ((:file "primitive")
             (:file "complex" :depends-on ("primitive"))
             (:module "parser"
                      :depends-on ("complex")
                      :components
                      ((:file "read")
                       (:file "canonicalize")
                       (:file "write" :depends-on ("canonicalize"))))
             (:file "fingerprint" :depends-on ("parser"))))
   (:module "io"
            :depends-on ("schema")
            :components
            ((:file "primitive")
             (:file "stream" :depends-on ("primitive"))
             (:file "complex" :depends-on ("stream"))
             (:file "resolve" :depends-on ("primitive"))))
   (:module "object-container-file"
            :depends-on ("io")
            :components
            ((:file "header")
             (:file "read" :depends-on ("header"))
             (:file "write" :depends-on ("header"))))
   (:file "single-object-encoding" :depends-on ("io"))))


(asdf:defsystem #:cl-avro/test
  :description "Tests for cl-avro."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:cl-avro #:1am)
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :pathname "test"
  :components
  ((:file "object-container-file")
   (:file "parser")
   (:file "resolve")
   (:file "fingerprint")
   (:file "single-object-encoding")))
