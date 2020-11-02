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

(defmacro rip-out-docstring (slots)
  (declare (symbol slots))
  `(let ((maybe-docstring (first ,slots)))
     (when (stringp maybe-docstring)
       (prog1 maybe-docstring
         (setf ,slots (cdr ,slots))))))

(defmacro rip-out-assertions (slots)
  (declare (symbol slots))
  `(let ((found (find 'with-assertions ,slots :test #'eq :key #'car-or-nil)))
     (when found
       (prog1 (cdr found)
         (setf ,slots (delete found ,slots :test #'eq :count 1))))))

(defunc assert-valid-slots (slots)
  "SLOTS is a list with elements that describe normal struct slots.

A slot looks like:
  (name initform type &optional predicatep)

and maps to a struct slot like:
  (name initform :type type :read-only t)

If predicatep is true, then a struct slot like the following will be defined:
  (namep nil :type boolean :read-only t)"
  (declare (list slots))
  (loop
    for slot in slots
    for (name initform type predicatep) = slot

    unless (member (length slot) '(3 4) :test #'=)
      do (error "Expected length of slot to be 3 or 4: ~S" slot)
    do
       (check-type name symbol)
       (check-type type (or symbol cons))
       (check-type predicatep boolean)))

(defunc private-constructor (name)
  (declare (symbol name))
  (intern (format nil "%MAKE-~S" name)))

(defunc public-constructor (name)
  (declare (symbol name))
  (intern (format nil "MAKE-~S" name)))

(defunc +p (name)
  (declare (symbol name))
  (intern (format nil "~SP" name)))

(defmacro with-slot-fields ((&rest fields) slot &body body)
  (let ((unused-fields '(name initform type predicatep)))
    (flet ((remove-used-fields (field)
             (unless (member field unused-fields :test #'eq)
               (error "Unknown field ~S" field))
             (setf unused-fields (remove field unused-fields :test #'eq))))
      (map nil #'remove-used-fields fields))
    `(destructuring-bind (name initform type &optional predicatep)
         ,slot
       (declare (ignore ,@unused-fields))
       ,@body)))

(defunc lambda-list (slots)
  (declare (list slots))
  (flet ((keyarg (slot)
           (with-slot-fields (name initform predicatep) slot
             (if predicatep
                 `(,name ,initform ,(+p name))
                 `(,name ,initform)))))
    `(&key ,@(mapcar #'keyarg slots))))

(defunc type-declarations (slots)
  (declare (list slots))
  (flet ((type-decl (slot)
           (with-slot-fields (name type) slot
             `(,type ,name))))
    (mapcar #'type-decl slots)))

(defunc check-types (slots)
  (declare (list slots))
  (flet ((check-type-form (slot)
           (with-slot-fields (name type) slot
             `(check-type ,name ,type))))
    (mapcar #'check-type-form slots)))

(defunc private-constructor-args (slots)
  (declare (list slots))
  (labels
      ((keyword-pair (symbol)
         `(,(intern (symbol-name symbol) 'keyword) ,symbol))
       (keyargs (slot)
         (with-slot-fields (name predicatep) slot
           (let ((keyargs (keyword-pair name)))
             (if predicatep
                 (nconc keyargs (keyword-pair (+p name)))
                 keyargs)))))
    (mapcan #'keyargs slots)))

(defunc defconstructor (name slots assertions)
  (declare (symbol name)
           (list slots assertions))
  (let ((lambda-list (lambda-list slots))
        (declarations (nconc (type-declarations slots)
                             (rip-out-declarations assertions)))
        (check-types (check-types slots))
        (public-ctor (public-constructor name))
        (private-ctor (private-constructor name))
        (private-ctor-args (private-constructor-args slots)))
    `(defun ,public-ctor ,lambda-list
       (declare ,@declarations)
       ,@check-types
       ,@assertions
       (,private-ctor ,@private-ctor-args))))

(defunc struct-slots (slots)
  (declare (list slots))
  (flet ((struct-slots (slot)
           (with-slot-fields (name initform type predicatep) slot
             (let ((struct-slot `(,name ,initform :type ,type :read-only t)))
               (if predicatep
                   `(,struct-slot (,(+p name) nil :type boolean :read-only t))
                   `(,struct-slot))))))
    (mapcan #'struct-slots slots)))

(defunc expand-initforms (slots)
  (declare (list slots))
  (flet ((expand-initform (slot)
           (with-slot-fields (name initform) slot
             (when (eq initform 'error)
               (let ((message (format nil "Must supply ~S" name)))
                 (rplaca (cdr slot) `(error ,message)))))))
    (mapc #'expand-initform slots)))

(defmacro defstruct+ ((name &optional super &rest super-slots) &body slots)
  "Define a struct along with validating constructor.

The first element of SLOTS may be a docstring for the defstruct, and
one of the elements may be a WITH-ASSERTIONS form.  All other elements
of SLOTS must adhere to ASSERT-VALID-SLOTS, with the exception that
the initform may be ERROR. An ERROR initform will be replaced
with (error \"Must supply SLOT-NAME\").

SUPER is a struct to inherit from, and SUPER-SLOTS are slots from
SUPER to override."
  (declare (symbol name)
           ((or null symbol) super))
  (let ((docstring (rip-out-docstring slots))
        (assertions (rip-out-assertions slots)))
    (assert-valid-slots slots)
    (assert-valid-slots super-slots)
    (expand-initforms slots)
    (expand-initforms super-slots)
    `(progn
       (defstruct (,name
                   (:constructor ,(private-constructor name))
                   (:copier nil)
                   ,@(when super `((:include ,super ,@(struct-slots super-slots)))))
         ,@(when docstring `(,docstring))
         ,@(struct-slots slots))

       ,(defconstructor name (append super-slots slots) assertions))))

;; provided for indentation handling and docstring
(defmacro with-assertions (&body assertions)
  "ASSERTIONS will be applied at the end of the constructor created by DEFSTRUCT+.

If the first form is a DECLARE, then its declarations will be added to
the constructor's."
  (declare (ignore assertions))
  (error "This is only supposed to be used within DEFSTRUCT+."))
