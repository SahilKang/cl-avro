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

(defmacro defunc (name (&rest args) &body body)
  "Defun a compile-time function."
  (declare (symbol name))
  `(eval-when (:compile-toplevel :execute)
     (defun ,name ,args
       ,@body)))

(defunc car-or-nil (maybe-cons)
  (when (consp maybe-cons)
    (car maybe-cons)))

(defmacro rip-out-declarations (body)
  (declare (symbol body))
  `(let* ((end (min 2 (length ,body)))
          (declare (find 'declare ,body :test #'eq :key #'car-or-nil :end end)))
     (when declare
       (prog1 (cdr declare)
         (setf ,body (delete declare ,body :test #'eq :count 1))))))

(defunc assert-valid-declarations (declarations)
  (declare (list declarations))
  (when (and (member 'inline declarations :test #'eq)
             (member 'inlinable declarations :test #'eq))
    (error "Only one of INLINE or INLINABLE can be declared")))

(defunc should-inline-p (declarations)
  (declare (list declarations))
  (or (member 'inline declarations :test #'eq)
      (member 'inlinable declarations :test #'eq)))

(defunc should-notinline-p (declarations)
  (declare (list declarations))
  (member 'inlinable declarations :test #'eq))

(defunc should-optimize-p (declarations)
  (declare (list declarations))
  (flet ((car-or-nil (maybe-cons)
           (when (consp maybe-cons)
             (car maybe-cons))))
    (not (member 'optimize declarations :test #'eq :key #'car-or-nil))))

(defunc filter-declarations (declarations)
  (declare (list declarations))
  (flet ((filter (elt)
           (unless (member elt '(inline inlinable) :test #'eq)
             (list elt))))
    (mapcan #'filter declarations)))

(defmacro defun+ (name (&rest args) ((&rest arg-types) result-type) &body body)
  (declare (symbol name))
  (let ((declarations (rip-out-declarations body))
        (declamations `((ftype (function ,arg-types ,result-type) ,name))))
    (assert-valid-declarations declarations)
    (when (should-inline-p declarations)
      (push `(inline ,name) declamations))
    (when (should-optimize-p declarations)
      (push '(optimize (speed 3) (safety 0)) declarations))
    `(progn
       (declaim ,@declamations)
       (defun ,name ,args
         (declare ,@(filter-declarations declarations))
         ,@body)
       ,@(when (should-notinline-p declarations)
           `((declaim (notinline ,name)))))))

(defmacro defun! (name (&rest args) ((&rest arg-types) result-type) &body body)
  (declare (symbol name))
  (let ((declarations (rip-out-declarations body)))
    (push 'inlinable declarations)
    `(defun+ ,name ,args
         (,arg-types ,result-type)
       (declare ,@declarations)
       ,@body)))

(defmacro defpreds
    ((name
      (input input-type)
      &optional (name! (intern (format nil "~S!" name))))
     &body body)
  (declare (symbol name input name!)
           ((or symbol cons) input-type))
  `(progn
     (defun! ,name! (,input)
         ((,input-type) boolean)
       ,@body)

     (defun! ,name (input)
         ((t) boolean)
       (declare (inline ,name!))
       (and (typep input ',input-type)
            (,name! input)))))

(defmacro deftype+ ((name (input base &rest moar)) &body body)
  (declare (symbol name input)
           ((or symbol cons) base))
  (let ((pred (intern (format nil "~S-P" name)))
        (bases `(,base ,@moar))
        (input-type (if (zerop (length moar))
                        base
                        `(and ,base ,@moar))))
    `(progn
       (defpreds (,pred (,input ,input-type))
         ,@body)

       (deftype ,name ()
         '(and ,@bases (satisfies ,pred))))))

(defmacro defseq ((name) &body body)
  (declare (symbol name))
  `(deftype+ (,name (vector simple-vector))
     ,@body))

(defmacro defvec (elt-name &optional (elt-type elt-name))
  (declare (symbol elt-name)
           ((or symbol cons) elt-type))
  (let ((type-name (intern (format nil "VECTOR[~S]" elt-name))))
    `(defseq (,type-name)
       (every (lambda (elt)
                (typep elt ',elt-type))
              vector))))

(defmacro defarray (name &optional (type name))
  (declare (symbol name)
           ((or symbol cons) type))
  (let ((array-name (intern (format nil "ARRAY[~S]" name))))
    `(deftype ,array-name (&optional size)
       `(simple-array ,',type (,size)))))

(defmacro %defset (elt-name elt-type test key)
  (declare (symbol elt-name)
           ((or symbol cons) elt-type test key))
  (when (symbolp test)
    (setf test `#',test))
  (let ((type-name (intern (format nil "SET[~S]" elt-name)))
        (get-key (gensym))
        (short-circuit (gensym)))
    `(defseq (,type-name)
       (labels
           ((,get-key (elt)
              (declare (,elt-type elt))
              ,(if (symbolp key)
                   `(,key elt)
                   (ecase (first key)
                     (lambda `(,key elt))
                     ((function quote) `(,(second key) elt)))))
            (,short-circuit (elt)
              (if (typep elt ',elt-type)
                  (cons t (,get-key elt))
                  (cons nil nil))))
         (loop
           with hash-table = (make-hash-table :test ,test :size (length vector))

           for elt across vector
           for (validp . key) = (,short-circuit elt)

           unless validp
             return nil

           when (gethash key hash-table)
             return nil

           do (setf (gethash key hash-table) t)

           finally (return t))))))

(defmacro defset (elt-spec test-spec)
  (declare ((or symbol cons) elt-spec test-spec))
  (when (consp test-spec)
    (case (first test-spec)
      (lambda (error "test must be suitable for hash-table: ~S" test-spec))
      ((function quote) (setf test-spec (second test-spec)))))
  (destructuring-bind (elt-name elt-type)
      (if (consp elt-spec)
          elt-spec
          (list elt-spec elt-spec))
    (destructuring-bind (test key)
        (if (consp test-spec)
            test-spec
            (list test-spec 'identity))
      `(%defset ,elt-name ,elt-type ,test ,key))))

(defmacro defenum ((name) &body values)
  (declare (symbol name))
  (unless (every (lambda (elt)
                   (simple-string-p elt))
                 values)
    (error "Expected simple-strings: ~S" values))
  (unless (= (length values)
             (length (remove-duplicates values :test #'string=)))
    (error "Enum strings must be unique: ~S" values))
  (let ((type-name (intern (format nil "ENUM[~S]" name))))
    ;; at some n, an O(1) hashed search is better than this linear
    ;; search...this can be determined and switched out at
    ;; compile-time
    `(deftype+ (,type-name (enum simple-string))
       (or ,@(mapcar (lambda (value)
                       `(string= enum ,value))
                     values)))))
