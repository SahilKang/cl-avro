# Changelog

## [1.0.2] - 2026-11-21

### Fixed

 * Work around a `compile-file` build error caused by a type declaration.
   [issue #59](https://github.com/SahilKang/cl-avro/issues/59) from
   [@quicklisp](https://github.com/quicklisp).

 * Remove `named-readtables` dependency, which was causing a build error when
   compiling the tests.

## [1.0.1] - 2026-02-11

### Fixed

 * Add empty `:use` argument to `defpackage` forms to avoid potential symbol
   conflicts with the `cl` package.
   [issue #57](https://github.com/SahilKang/cl-avro/issues/57) from
   [@Yehouda](https://github.com/Yehouda).

## [1.0.0] - 2025-10-06

Initial release
