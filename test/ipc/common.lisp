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
(defpackage #:cl-avro/test/ipc/common
  (:use #:cl)
  (:local-nicknames
   (#:avro #:cl-avro))
  (:export #:flatten))
(in-package #:cl-avro/test/ipc/common)

(declaim
 (ftype (function (cl-avro.internal.ipc.framing:buffers)
                  (values (simple-array (unsigned-byte 8) (*)) &optional))
        flatten))
(defun flatten (buffers)
  (flet ((fill-vector (vector buffer)
           (loop
             for byte across buffer
             do (vector-push-extend byte vector)
             finally (return vector))))
    (coerce
     (reduce
      #'fill-vector
      buffers
      :initial-value (make-array 0 :element-type '(unsigned-byte 8)
                                   :adjustable t :fill-pointer t))
     '(simple-array (unsigned-byte 8) (*)))))
