;;; Copyright (C) 2019-2020 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defmacro in-range-p (char-code start-char &optional end-char)
  "True if CHAR-CODE is between the char-code given by START-CHAR and END-CHAR.

If END-CHAR is nil, then determine if CHAR-CODE equals the char-code of
START-CHAR."
  (declare (character start-char)
           ((or null character) end-char))
  (let ((start (char-code start-char))
        (end (when end-char (char-code end-char))))
    (if end
        `(the boolean
              (and (>= ,char-code ,start)
                   (<= ,char-code ,end)))
        `(the boolean
              (= ,char-code ,start)))))

(macrolet
    ((defpred (name (start-char &optional end-char))
       (declare (symbol name))
       (when end-char
         (setf end-char (list end-char)))
       `(defun! ,name (char-code)
            ((fixnum) boolean)
          (in-range-p char-code ,start-char ,@end-char))))
  (defpred digit-p (#\0 #\9))
  (defpred uppercase-p (#\A #\Z))
  (defpred lowercase-p (#\a #\z))
  (defpred underscore-p (#\_)))

(deftype+ (avro-name-string (name simple-string))
  "True if NAME matches regex /^[A-Za-z_][A-Za-z0-9_]*$/ and nil otherwise."
  (declare (inline lowercase-p uppercase-p underscore-p digit-p))
  (unless (zerop (length name))
    (let ((first (char-code (schar name 0))))
      (declare (fixnum first))
      (when (or (lowercase-p first)
                (uppercase-p first)
                (underscore-p first))
        (loop
          for i of-type fixnum from 1 below (length name)
          for char-code of-type fixnum = (char-code (schar name i))

          always (or (lowercase-p char-code)
                     (uppercase-p char-code)
                     (underscore-p char-code)
                     (digit-p char-code)))))))

(eval-when (:compile-toplevel :execute)
  (defparameter +primitive-names+
    '("null" "boolean" "int" "long" "float" "double" "bytes" "string")))

(defmacro primitive-name-p (name)
  (declare (symbol name))
  `(or ,@(mapcar (lambda (primitive-name)
                   `(string= ,name ,primitive-name))
                 +primitive-names+)))

(deftype+ (avro-name (name simple-string))
  "True if NAME matches regex /^[A-Za-z_][A-Za-z0-9_]*$/ and nil otherwise.

Also nil if NAME matches a primitive name."
  (declare (inline avro-name-string-p!))
  (and (not (primitive-name-p name))
       (avro-name-string-p! name)))

(defmacro primitive-fullname-p (fullname)
  (declare (symbol fullname))
  (let ((name (gensym))
        (pos (gensym)))
    `(let ((,name ,fullname)
           (,pos (position #\. ,fullname :test #'char= :from-end t)))
       (when ,pos
         (setf ,name (subseq ,fullname (1+ ,pos))))
       (primitive-name-p ,name))))

(defun! %avro-fullname-p! (fullname)
    ((simple-string) boolean)
  (declare (inline lowercase-p uppercase-p underscore-p digit-p))
  (loop
    with prev-dot-p of-type boolean = t

    for char of-type character across fullname
    for char-code of-type fixnum = (char-code char)

    if (= char-code #.(char-code #\.))
      do (if prev-dot-p
             (return nil)
             (setf prev-dot-p t))
    else
      do (if prev-dot-p
             (unless (or (lowercase-p char-code)
                         (uppercase-p char-code)
                         (underscore-p char-code))
               (return nil))
             (unless (or (lowercase-p char-code)
                         (uppercase-p char-code)
                         (underscore-p char-code)
                         (digit-p char-code))
               (return nil)))
         (setf prev-dot-p nil)

    finally (return (not prev-dot-p))))

(deftype+ (avro-fullname (fullname simple-string))
  "True if FULLNAME is either an avro-name or dot-separated string of avro-names.

Also nil if unqualified FULLNAME matches a primitive name."
  (declare (inline %avro-fullname-p!))
  (unless (zerop (length fullname))
    (and (not (primitive-fullname-p fullname))
         (%avro-fullname-p! fullname))))

(defun! avro-namespace-p (namespace)
    ((t) boolean)
  "True if NAMESPACE is either nil, empty, or an avro-fullname."
  (declare (inline %avro-fullname-p!))
  (or (null namespace)
      (and (simple-string-p namespace)
           (or (zerop (length namespace))
               (%avro-fullname-p! namespace)))))

(deftype avro-namespace ()
  '(or null (and simple-string (satisfies avro-namespace-p))))


(defun! dot-join (prefix suffix)
    ((simple-string simple-string) simple-string)
  (let* ((prefix-length (length prefix))
         (suffix-length (length suffix))
         (output (make-string (1+ (+ prefix-length suffix-length)))))
    (declare (fixnum prefix-length suffix-length))
    (loop
      for i of-type fixnum upto prefix-length
      do (setf (schar output i) (schar prefix i)))
    (setf (schar output prefix-length) #\.)
    (loop
      for i of-type fixnum upto suffix-length
      for j of-type fixnum = (1+ prefix-length) then (1+ j)

      do (setf (schar output j) (schar suffix i))

      finally (return output))))

(defun! deduce-fullname (name namespace enclosing-namespace)
    ((avro-fullname avro-namespace avro-namespace) avro-fullname)
  (declare (inline dot-join))
  (cond
    ((find #\. name :test #'char=)
     name)
    ((and namespace (> (length namespace) 0))
     (dot-join namespace name))
    ((and enclosing-namespace (> (length enclosing-namespace) 0))
     (dot-join enclosing-namespace name))
    (t name)))

(defun! deduce-namespace (name namespace enclosing-namespace)
    ((avro-fullname avro-namespace avro-namespace) avro-namespace)
  (let ((pos (position #\. name :test #'char= :from-end t)))
    (cond
      (pos
       (subseq name 0 pos))
      ((and namespace (> (length namespace) 0))
       namespace)
      (t
       enclosing-namespace))))

(defun! unqualified-name (fullname)
    ((avro-fullname) avro-name)
  (let ((pos (position #\. fullname :test #'char= :from-end t)))
    (if pos
        (subseq fullname (1+ pos))
        fullname)))
