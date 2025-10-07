# cl-avro

[![tag](https://img.shields.io/github/tag/SahilKang/cl-avro.svg)](https://github.com/SahilKang/cl-avro/tags)
[![license](https://img.shields.io/badge/license-GPL%20v3-blue.svg)](https://github.com/SahilKang/cl-avro/blob/master/LICENSE)

A Common Lisp implementation of the
[Apache Avro](https://github.com/apache/avro) data serialization system.

# Public API

The public api is exported through the `cl-avro` and `cl-avro/asdf` packages:
to output their documentation, run `(documentation (find-package 'cl-avro) t)`
or `(documentation (find-package 'cl-avro/asdf) t)`.

# Running Tests

Tests can be run either through a lisp repl or docker.

## Lisp REPL

```lisp
(ql:quickload 'cl-avro/test)
(asdf:test-system 'cl-avro)
```

## Docker

```shell
docker build . -f ./test/Dockerfile.test -t cl-avro:v1

docker run -it --rm cl-avro:v1
```
