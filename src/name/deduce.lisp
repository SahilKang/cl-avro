;;; Copyright 2021, 2023 Google LLC
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
(defpackage #:cl-avro.internal.name.deduce
  (:use #:cl)
  (:local-nicknames
   (#:type #:cl-avro.internal.name.type))
  (:import-from #:cl-avro.internal.type
                #:ufixnum)
  (:export #:fullname->name
           #:deduce-namespace
           #:deduce-fullname
           #:null-namespace-p))
(in-package #:cl-avro.internal.name.deduce)

(declaim
 (ftype (function (type:fullname) (values (or null ufixnum) &optional))
        last-dot-position))
(defun last-dot-position (fullname)
  (position #\. fullname :test #'char= :from-end t))

;;; fullname->name

(declaim
 (ftype (function (type:fullname) (values type:name &optional)) fullname->name))
(defun fullname->name (fullname)
  "Return namespace unqualified name."
  (let ((last-dot-position (last-dot-position fullname)))
    (if last-dot-position
        (subseq fullname (1+ last-dot-position))
        fullname)))

;;; deduce-namespace

(declaim
 (ftype (function (type:namespace) (values boolean &optional))
        null-namespace-p))
(defun null-namespace-p (namespace)
  "True if NAMESPACE is either nil or empty."
  (or (null namespace)
      (zerop (length namespace))))

(declaim
 (ftype (function (type:fullname type:namespace type:namespace)
                  (values type:namespace &optional))
        deduce-namespace))
(defun deduce-namespace (fullname namespace enclosing-namespace)
  "Deduce namespace."
  (let ((last-dot-position (last-dot-position fullname)))
    (cond
      (last-dot-position
       (subseq fullname 0 last-dot-position))
      ((not (null-namespace-p namespace))
       namespace)
      (t
       enclosing-namespace))))

;;; deduce-fullname

(declaim
 (ftype (function ((and type:namespace (not null)) type:name)
                  (values type:fullname &optional))
        make-fullname))
(defun make-fullname (namespace name)
  (let* ((namespace-length (length namespace))
         (name-length (length name))
         (fullname (make-string
                    (+ 1 namespace-length name-length) :initial-element #\.)))
    (replace fullname namespace)
    (replace fullname name :start1 (1+ namespace-length))))

(declaim
 (ftype (function (type:fullname type:namespace type:namespace)
                  (values type:fullname &optional))
        deduce-fullname))
(defun deduce-fullname (fullname namespace enclosing-namespace)
  "Deduce fullname."
  (cond
    ((find #\. fullname :test #'char=)
     fullname)
    ((not (null-namespace-p namespace))
     (make-fullname namespace fullname))
    ((not (null-namespace-p enclosing-namespace))
     (make-fullname enclosing-namespace fullname))
    (t
     fullname)))
