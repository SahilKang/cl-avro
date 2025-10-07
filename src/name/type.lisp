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
(defpackage #:cl-avro.internal.name.type
  (:use #:cl)
  (:local-nicknames
   (#:ascii #:cl-avro.internal.ascii))
  (:import-from #:cl-avro.internal.type
                #:ufixnum)
  (:export #:name
           #:namespace
           #:fullname))
(in-package #:cl-avro.internal.name.type)

;;; name

(declaim
 (ftype (function (simple-string ufixnum ufixnum) (values boolean &optional))
        %name-p))
(defun %name-p (string start end)
  (unless (>= start end)
    (let ((first-char (char string start)))
      (when (or (ascii:letter-p first-char)
                (char= #\_ first-char))
        (loop
          for i of-type ufixnum from (1+ start) below end
          for char of-type character = (char string i)

          always (or (ascii:letter-p char)
                     (ascii:digit-p char)
                     (char= #\_ char)))))))

(declaim (ftype (function (t) (values boolean &optional)) name-p))
(defun name-p (string)
  "True if NAME matches regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  (when (simple-string-p string)
    (%name-p string 0 (length string))))

(deftype name ()
  "A string that matches the regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  '(and simple-string (satisfies name-p)))

;;; fullname

(declaim
 (ftype (function (simple-string) (values boolean &optional)) %fullname-p))
(defun %fullname-p (string)
  (loop
    with length = (length string)
    for start of-type ufixnum = 0 then (1+ end)
    for end of-type (or ufixnum null) = (position #\. string :start start)
    always (%name-p string start (or end length))
    while end))

(declaim (ftype (function (t) (values boolean &optional)) fullname-p))
(defun fullname-p (string)
  "True if FULLNAME is a nonempty, dot-separated string of NAMES."
  (when (simple-string-p string)
    (%fullname-p string)))

(deftype fullname ()
  "A nonempty, dot-separated string of NAMEs."
  '(and simple-string (satisfies fullname-p)))

;;; namespace

(declaim (ftype (function (t) (values boolean &optional)) namespace-p))
(defun namespace-p (string)
  "True if NAMESPACE is either nil, an empty string, or a FULLNAME."
  (or (null string)
      (when (simple-string-p string)
        (or (zerop (length string))
            (%fullname-p string)))))

(deftype namespace ()
  "Either nil, an empty string, or a FULLNAME."
  '(or null (and simple-string (satisfies namespace-p))))
