;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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
  :build-pathname "cl-avro"
  :components
  ((:file "package" :pathname "src/package")
   (:module "schema"
            :pathname "src/schema"
            :depends-on ("package")
            :components
            ((:file "primitive")
             (:file "complex" :depends-on ("primitive"))
             (:module "parser"
                      :depends-on ("primitive" "complex")
                      :components
                      ((:file "read")
                       (:file "write")))))
   (:module "io"
            :pathname "src/io"
            :depends-on ("schema")
            :components
            ((:file "primitive")
             (:file "stream" :depends-on ("primitive"))
             (:file "complex" :depends-on ("stream"))))
   (:module "object-container-file"
            :pathname "src/object-container-file"
            :depends-on ("io")
            :components
            ((:file "header")
             (:file "read" :depends-on ("header"))
             (:file "write" :depends-on ("header"))))))

(asdf:defsystem #:cl-avro/test
  :description "Tests for cl-avro."
  :version (:read-file-form "version.lisp")
  :author "Sahil Kang <sahil.kang@asilaycomputing.com>"
  :license "GPLv3"
  :depends-on (#:cl-avro #:1am)
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :components
  ((:module
    "test"
    :components
    ((:file "object-container-file")))))
