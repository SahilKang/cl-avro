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
(defpackage #:cl-avro.ipc.framing
  (:use #:cl)
  (:local-nicknames
   (#:schema #:cl-avro.schema)
   (#:io #:cl-avro.io)
   (#:handshake #:cl-avro.ipc.handshake))
  (:export #:frame
           #:to-input-stream
           #:input-stream
           #:buffers
           #:buffer))
(in-package #:cl-avro.ipc.framing)

(declaim ((unsigned-byte 32) +buffer-size+))
(defconstant +buffer-size+ (* 8 1024))

(declaim ((unsigned-byte 32) +max-object-size+))
(defconstant +max-object-size+ (- +buffer-size+ 4))

(deftype buffer ()
  '(simple-array (unsigned-byte 8) (*)))

(deftype buffers ()
  '(simple-array buffer (*)))

;;; frame

(declaim
 (ftype (function ((unsigned-byte 32)) (values buffer &optional)) make-buffer))
(defun make-buffer (size)
  (loop
    with buffer = (make-array (+ 4 size) :element-type '(unsigned-byte 8))

    for i below 4
    for shift = 24 then (- shift 8)
    for byte = (logand #xff (ash size (- shift)))

    do (setf (elt buffer i) byte)

    finally
       (return buffer)))

(declaim
 (ftype (function (schema:object) (values buffer &optional)) buffer))
(defun buffer (object)
  (let ((buffer (make-buffer (io:serialized-size object))))
    (io:serialize object :into buffer :start 4)
    buffer))

(declaim
 (ftype (function ((simple-array fixnum (*)) &optional fixnum)
                  (values buffers &optional))
        allocate-buffers))
(defun allocate-buffers (object-sizes &optional (prefix-pad 0))
  (loop
    with total-object-size = (reduce #'+ object-sizes :initial-value 0)
    with (filled-buffers remaining-bytes) = (multiple-value-list
                                             (truncate total-object-size
                                                       +max-object-size+))
    with buffers = (make-array (if (zerop remaining-bytes)
                                   (+ filled-buffers 1 prefix-pad)
                                   (+ filled-buffers 2 prefix-pad)))
      initially
         (when (plusp remaining-bytes)
           (setf (elt buffers filled-buffers) (make-buffer remaining-bytes)))

    for i from prefix-pad below filled-buffers
    do (setf (elt buffers i) (make-buffer +max-object-size+))

    finally
       (return buffers)))

(declaim
 (ftype (function (list (simple-array fixnum (*)) buffers &optional fixnum)
                  (values &optional)) frame-into))
(defun frame-into (objects object-sizes buffers &optional (start 0))
  (loop
    with buffer-index = start
    and start = 4

    for object in objects
    for object-size across object-sizes
    for buffer = (elt buffers buffer-index)

    if (> object-size (- (length buffer) start)) do
      (if (or (> object-size +max-object-size+)
              #+nil(= buffer-index (1- (length buffers)))
              (> object-size (- (length
                                 (elt buffers (1+ buffer-index)))
                                4)))
          ;; TODO instead of this copying which is also used in java
          ;; and python impls, I can also just have a large buffer for
          ;; large objects...the network cost might be higher though
          ;;
          ;; I should just add stream support to serialize
          (loop
            with serialized = (io:serialize object)
            and total-bytes-copied = 0

            until (= total-bytes-copied object-size)
            for buffer = (elt buffers buffer-index)
            do
               (replace buffer serialized
                        :start1 start :start2 total-bytes-copied)
               (let ((bytes-copied
                       (min (- (length buffer) start)
                            (- object-size total-bytes-copied))))
                 (incf total-bytes-copied bytes-copied)
                 (incf start bytes-copied)
                 (when (= start (length buffer))
                   (incf buffer-index)
                   (setf start 4))))
          (loop
            with borrowed-buffer = (elt buffers (1+ buffer-index))
            and bytes-copied = (- (length buffer) start)
            
                  initially
                     (io:serialize object :into borrowed-buffer :start 4)
                     (replace buffer borrowed-buffer :start1 start :start2 4)
                     (incf buffer-index)
                     (setf start (+ 4 (- object-size bytes-copied)))

            repeat (- object-size bytes-copied)
            for source = (+ bytes-copied 4) then (1+ source)
            for target = 4 then (1+ target)
            do (setf (elt borrowed-buffer target)
                     (elt borrowed-buffer source))))
    else do
      (io:serialize object :into buffer :start start)
      (incf start object-size)
      (when (= start (length buffer))
        (incf buffer-index)
        (setf start 4))

    finally
       (return (values))))

(declaim
 (ftype (function (list) (values (simple-array fixnum (*)) &optional))
        object-sizes))
(defun object-sizes (objects)
  (map '(simple-array fixnum (*)) #'io:serialized-size objects))

(declaim
 (ftype (function (handshake:request list) (values buffers &optional))
        frame-with-handshake))
(defun frame-with-handshake (handshake objects)
  (let* ((object-sizes (object-sizes objects))
         (buffers (allocate-buffers object-sizes 1)))
    (frame-into objects object-sizes buffers 1)
    (setf (elt buffers 0) (buffer handshake))
    buffers))

(declaim
 (ftype (function (&rest schema:object) (values buffers &optional)) frame))
(defun frame (&rest objects)
  (if (typep (first objects) 'handshake:request)
      (frame-with-handshake (first objects) (rest objects))
      (let* ((object-sizes (object-sizes objects))
             (buffers (allocate-buffers object-sizes)))
        (frame-into objects object-sizes buffers)
        buffers)))

;;; unframe

;; input-stream

(defclass input-stream (trivial-gray-streams:fundamental-binary-input-stream
                        trivial-gray-streams:trivial-gray-stream-mixin)
  ((buffers
    :initarg :buffers
    :reader buffers
    :type (vector (vector (unsigned-byte 8))))
   (buffers-index
    :accessor buffers-index
    :type (and (integer 0) fixnum))
   (buffer-index
    :accessor buffer-index
    :type (and (integer 0) fixnum)))
  (:default-initargs
   :buffers (error "Must supply BUFFERS")))

(defmethod initialize-instance :after
    ((instance input-stream) &key)
  (with-slots (buffers-index buffer-index) instance
    (setf buffers-index 0
          buffer-index 0)))

(defmethod stream-element-type
    ((instance input-stream))
  '(unsigned-byte 8))

(defmethod trivial-gray-streams:stream-read-byte
    ((instance input-stream))
  (with-slots (buffers buffers-index buffer-index) instance
    (tagbody
     begin
       (if (= buffers-index (length buffers))
           :eof
           (let ((buffer (elt buffers buffers-index)))
             (when (= buffer-index (length buffer))
               (incf buffers-index)
               (setf buffer-index 0)
               (go begin))
             (prog1 (elt buffer buffer-index)
               (incf buffer-index)))))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream input-stream)
     (vector simple-array)
     (start fixnum)
     (end fixnum)
     &key)
  (declare (buffer vector))
  (with-slots (buffers buffers-index buffer-index) stream
    (loop
      until (or (>= start end)
                (= buffers-index (length buffers)))
      for buffer = (elt buffers buffers-index)

      if (= buffer-index (length buffer)) do
        (incf buffers-index)
        (setf buffer-index 0)
      else do
        (let* ((needed (- end start))
               (remaining (- (length buffer) buffer-index))
               (count (min needed remaining)))
          (replace vector buffer :start1 start :start2 buffer-index)
          (incf buffer-index count)
          (incf start count))

      finally
         (return start))))

;; unframe

(declaim
 (ftype (function (buffer &optional fixnum)
                  (values (unsigned-byte 32) &optional))
        parse-big-endian))
(defun parse-big-endian (buffer &optional (start 0))
  (loop
    with integer = 0

    repeat 4
    for i = start then (1+ i)
    for byte = (elt buffer i)
    for shift = 24 then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return integer)))

(declaim
 (ftype (function (stream (simple-array (unsigned-byte 8) (4)))
                  (values (unsigned-byte 32) &optional))
        read-size))
(defun read-size (stream buffer)
  (unless (= (read-sequence buffer stream) 4)
    (error 'end-of-file :stream *error-output*))
  (parse-big-endian buffer))

(declaim
 (ftype (function (stream) (values input-stream &optional))
        stream->input-stream))
(defun stream->input-stream (stream)
  (loop
    with size-buffer = (make-array 4 :element-type '(unsigned-byte 8))
    and buffers
          = (make-array 0 :element-type '(simple-array (unsigned-byte 8) (*))
                          :adjustable t :fill-pointer t)

    for size = (read-size stream size-buffer)
    until (zerop size)

    for buffer = (make-array size :element-type '(unsigned-byte 8))

    if (= (read-sequence buffer stream) size) do
      (vector-push-extend buffer buffers)
    else do
      (error 'end-of-file :stream *error-output*)

    finally
       (return (make-instance 'input-stream :buffers buffers))))

(declaim
 (ftype (function ((vector (unsigned-byte 8))) (values input-stream &optional))
        bytes->input-stream))
(defun bytes->input-stream (bytes)
  (loop
    with index = 0
    and buffers = (make-array 0 :element-type '(vector (unsigned-byte 8))
                                :adjustable t :fill-pointer t)

    for size = (prog1 (parse-big-endian bytes index)
                 (incf index 4))
    until (zerop size)

    for slice = (prog1 (make-array size
                                   :element-type '(unsigned-byte 8)
                                   :displaced-to bytes
                                   :displaced-index-offset index)
                  (incf index size))

    do (vector-push-extend slice buffers)

    finally
       (return (make-instance 'input-stream :buffers buffers))))

(declaim
 (ftype (function ((or (vector (unsigned-byte 8)) stream))
                  (values input-stream &optional))
        to-input-stream))
(defun to-input-stream (input)
  (if (streamp input)
      (stream->input-stream input)
      (bytes->input-stream input)))

#+nil
(declaim
 (ftype (function ((or buffer stream) &rest schema:schema)
                  (values &rest schema:object))
        unframe))
#+nil
(defun unframe (input &rest schemas)
  (let ((input-stream (to-input-stream input)))
    (flet ((deserialize (schema)
             (io:deserialize schema input-stream)))
      (values-list
       (mapcar #'deserialize schemas)))))
