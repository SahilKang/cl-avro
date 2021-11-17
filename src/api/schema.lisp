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

(export '(schema object))

(export 'schema-of)
(defgeneric schema-of (object))

(export '(fingerprint *default-fingerprint-algorithm*))

;;; primitive

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow '(boolean null double float string)))

(export '(primitive-schema primitive-object))

(export '(boolean true false))

(export 'bytes)

(export 'double)

(export 'float)

(export 'int)

(export 'long)

(export 'null)

(export 'string)

;;; complex

(export '(complex-schema complex-object))

;; array

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow '(array push pop)))

(export '(array items))
(export '(array-object raw))

(export 'push)
(defgeneric push (element array))

(export 'pop)
(defgeneric pop (array))

;; map

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow '(map values
            hash-table-count hash-table-size clrhash maphash gethash remhash)))

(export '(map values))
(export '(map-object raw))

(export 'hash-table-count)
(defgeneric hash-table-count (map))

(export 'hash-table-size)
(defgeneric hash-table-size (map))

(export 'clrhash)
(defgeneric clrhash (map))

(export 'maphash)
(defgeneric maphash (function map))

(export 'gethash)
(defgeneric gethash (key map &optional default))
(defgeneric (setf gethash) (value key map))

(export 'remhash)
(defgeneric remhash (key map))

;; union

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow '(union)))

(export '(union union-object schemas))

(export 'which-one)
(defgeneric which-one (object))

;; fixed

(export '(fixed fixed-object size))

;; enum

(export '(enum enum-object symbols))

(export 'default)
(defgeneric default (object))

;; record

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow '(ignore type)))

(export '(field name aliases type order default ascending descending ignore))
(defgeneric order (object))
(defgeneric type (object))

(export '(record record-object fields))
(defgeneric fields (object))

;;; logical

(export '(logical-schema logical-object))

(export 'uuid)

(export 'date)

(export 'year)
(defgeneric year (object &key &allow-other-keys))

(export 'month)
(defgeneric month (object &key &allow-other-keys))

(export 'day)
(defgeneric day (object &key &allow-other-keys))

(export '(time-millis time-micros))

(export 'hour)
(defgeneric hour (object &key &allow-other-keys))

(export 'minute)
(defgeneric minute (object &key &allow-other-keys))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (shadow 'second))

(export 'second)
(defgeneric second (object &key &allow-other-keys)
  (:documentation "Return (values second remainder)."))

(export 'millisecond)
(defgeneric millisecond (object &key &allow-other-keys))

(export 'microsecond)
(defgeneric microsecond (object &key &allow-other-keys))

(export '(timestamp-millis timestamp-micros))

(export '(local-timestamp-millis local-timestamp-micros))

(export '(decimal decimal-object precision unscaled))

(export 'scale)
(defgeneric scale (schema))

(export '(duration duration-object))

(export 'months)
(defgeneric months (object))

(export 'days)
(defgeneric days (object))

(export 'milliseconds)
(defgeneric milliseconds (object))

;;; internal

(in-package #:cl-avro.internal)

(export 'fixed-size)
(defgeneric fixed-size (schema))

(export 'serialize-field-default)
(defgeneric serialize-field-default (default))

(export 'deserialize-field-default)
(defgeneric deserialize-field-default (schema default))

(export 'skip)
(defgeneric skip (schema input &optional start))

(export 'underlying)
(defgeneric underlying (schema))
