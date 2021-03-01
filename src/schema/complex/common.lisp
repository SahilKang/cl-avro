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
(defpackage #:cl-avro.schema.complex.common
  (:use #:cl)
  (:export #:assert-distinct
           #:which-one
           #:default))
(in-package #:cl-avro.schema.complex.common)

;;; assert-distinct

(declaim
 (ftype (function (sequence) (values (vector string) &optional)) duplicates))
(defun duplicates (sequence)
  "Return duplicate strings found in SEQUENCE."
  (let ((seen (make-hash-table :test #'equal))
        (duplicates (make-array 0 :element-type 'string
                                  :adjustable t :fill-pointer 0)))
    (flet ((process (elt)
             (if (gethash elt seen)
                 (vector-push-extend elt duplicates)
                 (setf (gethash elt seen) t))))
      (map nil #'process sequence))
    (delete-duplicates duplicates :test #'string=)))

(declaim (ftype (function (sequence) (values &optional)) assert-distinct))
(defun assert-distinct (sequence)
  "Assert SEQUENCE does not contain duplicate strings."
  (let ((duplicates (duplicates sequence)))
    (unless (zerop (length duplicates))
      (error "Found duplicates: ~S" duplicates)))
  (values))

;;; which-one

(defgeneric which-one (object))

;;; default

(defgeneric default (object))
