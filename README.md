# cl-avro

[![license](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://github.com/SahilKang/cl-avro/blob/master/LICENSE)

A Common Lisp implementation of the
[Apache Avro](https://github.com/apache/avro) data serialization system.

This was written against the
[Avro 1.9.0 spec](https://avro.apache.org/docs/current/spec.html)
and currently adheres to the
[happy-path](https://en.wikipedia.org/wiki/Happy_path)
so there are a few TODOs:

* [aliases](https://avro.apache.org/docs/current/spec.html#Aliases)
are ignored.

* [default values](https://avro.apache.org/docs/current/spec.html#schema_record)
are ignored.

* [sort order](https://avro.apache.org/docs/current/spec.html#order)
is ignored.

* [object container](https://avro.apache.org/docs/current/spec.html#Object+Container+Files)
files are not supported.

* [schema resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
is not suppored.

# Examples

```lisp
(ql:quickload '(:cl-avro :trivial-gray-streams))

(use-package :trivial-gray-streams)

;; create in-memory stream classes:

(defclass input-stream (fundamental-binary-input-stream)
  ((bytes
    :initform (error "Must supply :bytes vector")
    :initarg :bytes)
   (position
    :initform 0)))

(defmethod stream-read-byte ((stream input-stream))
  (with-slots (bytes position) stream
    (if (= position (length bytes))
        :eof
        (prog1 (elt bytes position)
          (incf position)))))

(defclass output-stream (fundamental-binary-output-stream)
  ((bytes
    :initform (make-array 0
                          :element-type '(unsigned-byte 8)
                          :adjustable t
                          :fill-pointer 0)
    :reader get-bytes)))

(defmethod stream-write-byte ((stream output-stream) (byte integer))
  (check-type byte (unsigned-byte 8))
  (with-slots (bytes) stream
    (vector-push-extend byte bytes))
  byte)


(defparameter *schema*
  (avro:parse-schema
   "{\"type\": \"record\",
      \"name\": \"LongList\",
      \"doc\": \"A linked-list of 64-bit values\",
      \"aliases\": [\"LinkedLongs\"],
      \"fields\": [
        {\"name\": \"value\", \"type\": \"long\"},
        {\"name\": \"next\", \"type\": [\"null\", \"LongList\"]},
      ]
     }"))

(avro:validp *schema* '(2 (4 (6 (8 (10 nil))))))
;; => t

(avro:validp *schema* '(2 (4 (6 (#.(1- (expt 2 63)) (10 nil))))))
;; => t

(avro:validp *schema* '(2 (4 (6 (#.(expt 2 63) (10 nil))))))
;; => nil

(let* ((output-stream (make-instance 'output-stream))
       (input-stream (make-instance 'input-stream
                                    :bytes (get-bytes output-stream))))
  (avro:serialize output-stream *schema* '(2 (4 (6 (8 (10 nil))))))
  (avro:deserialize input-stream *schema*))
;; => #(2 #(4 #(6 #(8 #(10 NIL)))))
```
