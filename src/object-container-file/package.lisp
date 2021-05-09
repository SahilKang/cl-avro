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

(defpackage #:cl-avro.object-container-file
  (:use #:cl-avro.object-container-file.header
        #:cl-avro.object-container-file.file-block
        #:cl-avro.object-container-file.file-input-stream
        #:cl-avro.object-container-file.file-output-stream)
  (:export #:header
           #:magic
           #:meta
           #:sync

           #:schema
           #:codec

           #:null
           #:deflate
           #:snappy
           #:bzip2
           #:xz
           #:zstandard

           #:file-block
           #:count
           #:bytes

           #:file-input-stream
           #:skip-block
           #:read-block

           #:file-output-stream
           #:write-block))
