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

(in-package #:cl-avro)

;;; file-header

(export '(magic meta sync file-header))

(export 'schema)
(defgeneric schema (instance))

(export 'codec)
(defgeneric codec (instance))

;;; file-block

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow 'count))

(export '(file-block count))

(export '(*decompress-deflate* *decompress-bzip2* *decompress-snappy*
          *decompress-xz* *decompress-zstandard*))

(export '(*compress-deflate* *compress-bzip2* *compress-snappy*
          *compress-xz* *compress-zstandard*))

;;; file-reader

(export '(file-reader skip-block read-block))

;;; file-writer

(export '(file-writer write-block))
