;;; Copyright 2023 Google LLC
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
(defpackage #:cl-avro/asdf
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro))
  (:export #:avro-file))
(in-package #:cl-avro/asdf)

(defclass avro-file (asdf:file-component)
  ((processed-p
    :type boolean
    :accessor processed-p
    :initarg :processed-p))
  (:default-initargs
   :type nil
   :processed-p nil)
  (:documentation
   "ASDF component representing avro schema or protocol files.

Only json files are supported."))

(deftype filespec ()
  '(or string pathname))

(declaim
 (ftype (function (filespec) (values simple-string &optional)) read-file))
(defun read-file (filespec)
  (with-open-file (stream filespec :external-format :utf-8)
    (let* ((length (file-length stream))
           (string (make-string length)))
      (assert (= (read-sequence string stream) length))
      string)))

(declaim
 (ftype (function (filespec) (values (or api:schema api:protocol) &optional))
        deserialize))
(defun deserialize (filespec)
  (let* ((unparsed (read-file filespec))
         (jso (st-json:read-json unparsed t)))
    (if (nth-value 1 (st-json:getjso "protocol" jso))
        (api:deserialize 'api:protocol unparsed)
        (api:deserialize 'api:schema unparsed))))

(declaim
 (ftype (function (asdf:operation avro-file) (values &optional)) process))
(defun process (operation component)
  (unless (processed-p component)
    (loop
      for input-file in (asdf:input-files operation component)
      do (api:intern (deserialize input-file)))
    (setf (processed-p component) t))
  (values))

(defmethod asdf:perform ((operation asdf:compile-op) (component avro-file))
  (process operation component))

(defmethod asdf:perform ((operation asdf:load-op) (component avro-file))
  (process operation component))

(defmethod asdf:perform ((operation asdf:load-source-op) (component avro-file))
  (process operation component))

(setf (find-class 'asdf::avro-file) (find-class 'avro-file))
