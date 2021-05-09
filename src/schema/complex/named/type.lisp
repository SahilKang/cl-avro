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
(defpackage #:cl-avro.schema.complex.named.type
  (:use #:cl)
  (:import-from #:cl-avro.schema.ascii
                #:letter-p
                #:digit-p)
  (:export #:name
           #:namespace
           #:fullname

           #:fullname->name
           #:deduce-namespace
           #:deduce-fullname))
(in-package #:cl-avro.schema.complex.named.type)

;;; types

;; name

(declaim (ftype (function (t) (values boolean &optional)) name-p))
(defun name-p (name)
  "True if NAME matches regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  (when (and (simple-string-p name)
             (not (zerop (length name))))
    (let ((first (char name 0)))
      (when (or (letter-p first)
                (char= #\_ first))
        (loop
          for i from 1 below (length name)
          for char = (char name i)

          always (or (letter-p char)
                     (digit-p char)
                     (char= #\_ char)))))))

(deftype name ()
  "A string that matches the regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  '(and simple-string (satisfies name-p)))

;; fullname

(declaim (ftype (function (t) (values boolean &optional)) fullname-p))
(defun fullname-p (fullname)
  "True if FULLNAME is a nonempty, dot-separated string of NAMES."
  (when (and (simple-string-p fullname)
             (not (zerop (length fullname))))
    (every #'name-p (uiop:split-string fullname :separator "."))))

(deftype fullname ()
  "A nonempty, dot-separated string of NAMEs."
  '(and simple-string (satisfies fullname-p)))

;; namespace

(declaim (ftype (function (t) (values boolean &optional)) namespace-p))
(defun namespace-p (namespace)
  "True if NAMESPACE is either nil, an empty string, or a FULLNAME."
  (or (null namespace)
      (and (simple-string-p namespace)
           (or (zerop (length namespace))
               (fullname-p namespace)))))

(deftype namespace ()
  "Either nil, an empty string, or a FULLNAME."
  '(or null (and simple-string (satisfies namespace-p))))

;;; functions

;; name

(declaim
 (ftype (function (fullname) (values (or null (integer 0)) &optional))
        last-dot-position)
 (inline last-dot-position))
(defun last-dot-position (fullname)
  (position #\. fullname :test #'char= :from-end t))
(declaim (notinline last-dot-position))

(declaim
 (ftype (function (fullname) (values name &optional)) fullname->name)
 (inline fullname->name))
(defun fullname->name (fullname)
  "Return namespace unqualified name."
  (declare (inline last-dot-position))
  (let ((last-dot-position (last-dot-position fullname)))
    (if last-dot-position
        (subseq fullname (1+ last-dot-position))
        fullname)))
(declaim (notinline fullname->name))

;; namespace

(declaim
 (ftype (function (namespace) (values boolean &optional)) null-namespace-p))
(defun null-namespace-p (namespace)
  "True if NAMESPACE is either nil or empty."
  (or (null namespace)
      (zerop (length namespace))))

(declaim
 (ftype (function (fullname namespace namespace) (values namespace &optional))
        deduce-namespace))
(defun deduce-namespace (name namespace enclosing-namespace)
  "Deduce namespace."
  (let ((last-dot-position (last-dot-position name)))
    (cond
      (last-dot-position
       (subseq name 0 last-dot-position))
      ((not (null-namespace-p namespace))
       namespace)
      (t
       enclosing-namespace))))

;; fullname

(declaim
 (ftype (function (namespace name) (values fullname &optional)) dot-join))
(defun dot-join (namespace name)
  (format nil "~A.~A" namespace name))

(declaim
 (ftype (function (fullname namespace namespace) (values fullname &optional))
        deduce-fullname))
(defun deduce-fullname (name namespace enclosing-namespace)
  "Deduce fullname."
  (cond
    ((find #\. name :test #'char=)
     name)
    ((not (null-namespace-p namespace))
     (dot-join namespace name))
    ((not (null-namespace-p enclosing-namespace))
     (dot-join enclosing-namespace name))
    (t
     name)))
