;;; Copyright (C) 2019-2021 Sahil Kang <sahil.kang@asilaycomputing.com>
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
(defpackage #:cl-avro.schema.ascii
  (:use #:cl)
  (:export #:digit-p
           #:letter-p
           #:hex-p))
(in-package #:cl-avro.schema.ascii)

(defmacro between (code start end)
  "Determines if CODE is between the char-codes of START and END, inclusive."
  (declare (symbol code)
           (character start end))
  (let ((start (char-code start))
        (end (char-code end)))
    `(and (>= ,code ,start)
          (<= ,code ,end))))

(declaim
 (ftype (function (character) (values boolean &optional)) digit-p)
 (inline digit-p))
(defun digit-p (digit)
  "True if DIGIT is a base-10 digit."
  (let ((code (char-code digit)))
    (not (null (between code #\0 #\9)))))
(declaim (notinline digit-p))

(declaim
 (ftype (function (character) (values boolean &optional)) letter-p)
 (inline letter-p))
(defun letter-p (letter)
  "True if LETTER is a an ascii letter conforming to /[a-zA-Z]/"
  (let ((code (char-code letter)))
    (not (null (or (between code #\a #\z)
                   (between code #\A #\Z))))))
(declaim (notinline letter-p))

(declaim
 (ftype (function (character) (values boolean &optional)) hex-p)
 (inline hex-p))
(defun hex-p (hex)
  "True if HEX is a hex digit conforming to /[0-9a-f-A-F]/"
  (let ((code (char-code hex)))
    (not (null (or (between code #\0 #\9)
                   (between code #\a #\f)
                   (between code #\A #\F))))))
(declaim (notinline hex-p))
