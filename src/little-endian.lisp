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
(defpackage #:cl-avro.little-endian
  (:use #:cl)
  (:export #:to-uint32
           #:to-uint64
           #:from-uint32
           #:from-uint64))
(in-package #:cl-avro.little-endian)

;;; to-uint

(defmacro to-uint (bits input &optional (start nil startp))
  (declare ((member 32 64) bits)
           (symbol input start))
  (let ((integer (gensym))
        (index (gensym))
        (end `(+ ,start ,(truncate bits 8)))
        (shift (gensym))
        (byte (gensym)))
    `(loop
       with ,integer of-type (unsigned-byte ,bits) = 0

       ,@(when startp
           `(for ,index of-type fixnum from ,start below ,end))
       for ,shift of-type fixnum below ,bits by 8
       for ,byte of-type (unsigned-byte 8) = ,(if startp
                                                  `(elt ,input ,index)
                                                  `(read-byte ,input))

       do (setf ,integer (logior ,integer (ash ,byte ,shift)))

       finally
          (return ,integer))))

(defmacro define-to-uint-function (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "TO-UINT~A" bits))))
    `(progn
       (declaim
        (ftype (function ((or stream (simple-array (unsigned-byte 8) (*)))
                          &optional (and (integer 0) fixnum))
                         (values (unsigned-byte ,bits) &optional))
               ,name))
       (defun ,name (input &optional (start 0))
         (etypecase input
           (stream
            (to-uint ,bits input))
           ((simple-array (unsigned-byte 8) (*))
            (to-uint ,bits input start)))))))

(define-to-uint-function 32)

(define-to-uint-function 64)

;;; from-uint

(defmacro from-uint (bits integer into &optional (start nil startp))
  (declare ((member 32 64) bits)
           (symbol integer into start))
  (let ((shift (gensym))
        (byte (gensym))
        (index (gensym))
        (end `(+ ,start ,(truncate bits 8))))
    `(loop
       for ,shift of-type fixnum from 0 above ,(* -1 bits) by 8
       for ,byte of-type (unsigned-byte 8) = (logand #xff (ash ,integer ,shift))
       ,@(when startp
           `(for ,index of-type fixnum from ,start below ,end))

       do ,(if startp
               `(setf (elt ,into ,index) ,byte)
               `(write-byte ,byte ,into))

       finally
          (return (values)))))

(defmacro define-from-uint-function (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern (format nil "FROM-UINT~A" bits))))
    `(progn
       (declaim
        (ftype (function ((unsigned-byte ,bits)
                          (or stream (simple-array (unsigned-byte 8) (*)))
                          &optional (and (integer 0) fixnum))
                         (values &optional))
               ,name))
       (defun ,name (integer into &optional (start 0))
         (etypecase into
           (stream
            (from-uint ,bits integer into))
           ((simple-array (unsigned-byte 8) (*))
            (from-uint ,bits integer into start)))))))

(define-from-uint-function 32)

(define-from-uint-function 64)
