# cl-avro

[![CircleCI](https://circleci.com/gh/SahilKang/cl-avro.svg?style=shield)](https://circleci.com/gh/SahilKang/cl-avro)
[![license](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://github.com/SahilKang/cl-avro/blob/master/LICENSE)

A Common Lisp implementation of the
[Apache Avro](https://github.com/apache/avro) data serialization system.

This was written against the
[Avro 1.9.0 spec](https://avro.apache.org/docs/1.9.0/spec.html)
and there are a few TODOS:

* [sort order](https://avro.apache.org/docs/1.9.0/spec.html#order)
is not supported.

* [Avro RPC](https://avro.apache.org/docs/1.9.0/spec.html#Protocol+Declaration)
is not implemented.

# Examples

```lisp
(ql:quickload :cl-avro)

(defparameter *schema*
  (avro:json->schema
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

(avro:deserialize
 (avro:serialize nil *schema* '(2 (4 (6 (8 (10 nil))))))
 *schema*)
;; => #(2 #(4 #(6 #(8 #(10 NIL)))))
```
