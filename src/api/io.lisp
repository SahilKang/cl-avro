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

(export 'serialize)
(defgeneric serialize (object &key &allow-other-keys))

(export 'deserialize)
(defgeneric deserialize (schema input &key &allow-other-keys))

(export 'serialized-size)
(defgeneric serialized-size (object))

(in-package #:cl-avro.internal)

(export 'serialize)
(defgeneric serialize (object into &key &allow-other-keys))

(export 'crc-64-avro-little-endian)
(defgeneric crc-64-avro-little-endian (schema))

(export '(read-jso with-initargs))

(export 'write-jso)
(defgeneric write-jso (schema seen canonical-form-p))

(export 'logical-name)
(defgeneric logical-name (schema))

(export 'downcase-symbol)
