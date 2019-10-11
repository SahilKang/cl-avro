;;; Copyright (C) 2019 Sahil Kang <sahil.kang@asilaycomputing.com>
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

(defun deduce-fullname (name namespace enclosing-namespace)
  (cond
    ((position #\. name)
     name)
    ((not (zerop (length namespace)))
     (concatenate 'string namespace "." name))
    ((not (zerop (length enclosing-namespace)))
     (concatenate 'string enclosing-namespace "." name))
    (t name)))

(defun deduce-namespace (name namespace enclosing-namespace)
  (let ((pos (position #\. name :from-end t)))
    (cond
      (pos
       (subseq name 0 pos))
      ((not (zerop (length namespace)))
       namespace)
      (t
       enclosing-namespace))))
