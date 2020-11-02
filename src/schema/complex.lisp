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

(defstruct (complex-schema (:constructor nil)
                           (:copier nil))
  "Base class for avro complex schemas.")

(defarray avro-fullname)

(eval-when (:compile-toplevel :execute)
    (defparameter +named-slots+
      '((name error avro-fullname)
        (namespace nil avro-namespace t)
        (aliases #() array[avro-fullname] t))))

(macrolet
    ((defnamed ()
       `(defstruct (named-schema (:constructor nil)
                                 (:copier nil)
                                 (:include complex-schema))
          "Base class for avro named schemas."
          ,@(struct-slots (expand-initforms (copy-tree +named-slots+))))))
  (defnamed))

(defstruct (logical-schema (:constructor nil)
                           (:copier nil))
  "Base class for avro logical schemas with underlying schemas."
  (underlying-schema
   (error "Must supply underlying-schema")
   :type (or primitive-schema complex-schema)
   :read-only t))

(deftype avro-schema ()
  '(or primitive-schema complex-schema logical-alias logical-schema))

;;; avro complex schemas

(defunc assert-valid-attributes (attributes)
  "ATTRIBUTES must be a list containing only symbols: NAMED or DOC."
  (declare (list attributes))
  (let ((known '(named doc)))
    (flet ((assert-valid (attribute)
             (unless (member attribute known :test #'eq)
               (error "Unknown attribute ~S, expected one of ~S" attribute known))))
      (map nil #'assert-valid attributes))))

(defunc append-slot (slots slot)
  (declare (list slots slot))
  (rplacd (last slots) (list slot)))

(defmacro defcomplex (name (&rest attributes) &body slots)
  "Define a complex schema named NAME.

ATTRIBUTES must adhere to ASSERT-VALID-ATTRIBUTES.

SLOTS must adhere to DEFSTRUCT+."
  (declare (symbol name))
  (assert-valid-attributes attributes)
  (when (member 'doc attributes :test #'eq)
    (append-slot slots '(doc "" string-schema t)))
  (let ((super (if (member 'named attributes :test #'eq)
                   `(named-schema ,@+named-slots+)
                   '(complex-schema))))
    `(defstruct+ (,name ,@super)
       ,@slots)))

;; fixed-schema

(defcomplex fixed-schema (named)
  "Represents an avro fixed schema."
  (size error (integer 0)))

(defmethod assert-valid ((schema fixed-schema) (array simple-array))
  (declare (optimize (speed 3) (safety 0)))
  (check-type array array[byte])
  (let ((expected (fixed-schema-size schema))
        (actual (length array)))
    (unless (= expected actual)
      (error "Array's length is ~S instead of ~S" actual expected))))

;; union-schema

(defun! fullname (schema enclosing-namespace)
    ((named-schema avro-namespace) avro-fullname)
  (deduce-fullname (named-schema-name schema)
                   (named-schema-namespace schema)
                   enclosing-namespace))

(defun! schema-key (schema)
    ((avro-schema) (or symbol simple-string))
  (declare (inline fullname)
           (special *namespace*))
  (typecase schema
    (symbol schema)
    (named-schema (fullname schema *namespace*))
    (t (type-of schema))))

(defarray avro-schema)

(defcomplex union-schema ()
  "Represents an avro union schema."
  (schemas error array[avro-schema])
  (with-assertions
    (when (zerop (length schemas))
      (error "Schemas cannot be empty"))
    (let ((hash-table (make-hash-table :test #'equal :size (length schemas))))
      (labels
          ((assert-unique (schema)
             (declare (avro-schema schema)
                      (inline schema-key))
             (let ((key (schema-key schema)))
               (when (gethash key hash-table)
                 (error "Duplicate ~S schema in union" key))
               (setf (gethash key hash-table) t)))
           (assert-valid (schema)
             (declare (avro-schema schema))
             (when (union-schema-p schema)
               (error "Nested union schema: ~S" schema))
             (assert-unique schema)))
        (map nil #'assert-valid schemas)))))

;; TODO instead of carrying the entire schema around (which makes the
;; schema arg redudant), tagged-union should only hang onto the
;; schema-key
(defstruct+ (tagged-union)
  "Represents a value adhering to a union-schema."
  (value error t)
  (schema error avro-schema)
  (with-assertions
    (assert-valid schema value)))

(defmethod assert-valid ((schema union-schema) (tagged-union tagged-union))
  (declare (inline schema-key)
           (optimize (speed 3) (safety 0)))
  (let* ((schemas (union-schema-schemas schema))
         (key (schema-key (tagged-union-schema tagged-union)))
         (match (find key schemas :test #'equal :key #'schema-key)))
    (unless match
      (error "No matching schema for ~S" key))
    (assert-valid match (tagged-union-value tagged-union))))

;; array-schema

(defcomplex array-schema ()
  "Represents an avro array schema."
  (items error avro-schema))

;; TODO using a simple-array here leads to using deftypes for complex
;; types as well

;; name can be symbol or string
(deffixed name
    (namespace "...")
  (aliases "foo" "bar" "baz")
  (size 12))
...
will use fullname for uniqueness checking, but use only name for struct name
(defstruct (name (:include fixed-schema))
  (name "name")
  (namespace "...")
  (aliases "foo" "bar" "baz")
  (size 12))
...
instances of name should be arrays, though...so defstruct this instead
(defstruct (name (:include fixed-schema))
  (value of-type (simple-array (unsigned-byte 8) (12))))
... or this
(deftype name ()
  '(simple-array (unsigned-byte 8) (12)))
but this schema has to be serialiazable as json as well
...
can either use macros and store state myself, or utilize mop
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defmethod assert-valid ((schema array-schema) (vector simple-vector))
  (declare (optimize (speed 3) (safety 0)))
  (let ((items (array-schema-items schema)))
    (flet ((assert-valid (elt)
             (assert-valid items elt)))
      (map nil #'assert-valid vector))))

;; map-schema

(defcomplex map-schema ()
  "Represents an avro map schema."
  (values error avro-schema))

(defmethod assert-valid ((schema map-schema) (hash-table hash-table))
  (declare (optimize (speed 3) (safety 0)))
  (let ((test (hash-table-test hash-table)))
    (unless (eq test 'equal)
      (error "Hash-table's test is ~S, not #'equal" test)))
  (let ((values (map-schema-values schema)))
    (maphash (lambda (key value)
               (unless (simple-string-p key)
                 (error "Expected string for key: ~S" key))
               (assert-valid values value))
             hash-table)))

;; enum-schema

(defset avro-name-string #'equal)

(defcomplex enum-schema (named doc)
  "Represents an avro enum schema."
  (symbols error set[avro-name-string])
  (default nil (or null-schema avro-name-string))
  (with-assertions
    (when (and default (not (find default symbols :test #'string=)))
      (error "Default enum ~S is not in symbols ~S" default symbols))))

(defmethod assert-valid ((schema enum-schema) (enum simple-string))
  (declare (optimize (speed 3) (safety 0)))
  (let ((symbols (enum-schema-symbols schema)))
    (unless (find enum symbols :test #'string=)
      (error "Unknown enum ~S" enum))))

;; record-schema

(defun! string->vector (string)
    ((simple-string) vector[byte])
  (let ((bytes (babel:string-to-octets string :encoding :latin-1)))
    (declare ((simple-array (unsigned-byte 8)) bytes))
    (coerce bytes 'vector[byte])))

(defun! assert-valid-default (type default)
    ((avro-schema t) (values))
  (declare (inline string->vector))
  (typecase type
    (symbol
     (when (and (eq type 'bytes-schema)
                (simple-string-p default))
       (setf default (string->vector default)))
     (assert-valid type default))
    (fixed-schema
     (when (simple-string-p default)
       (setf default (string->vector default)))
     (assert-valid type default))
    (union-schema
     (let ((schemas (union-schema-schemas type)))
       (assert-valid (svref schemas 0) default)))
    (t
     (assert-valid type default))))

(defenum (ordering) "ascending" "descending" "ignore")

(defstruct+ (field-schema)
  "Represents the schema of a field found within an avro record schema."
  (name error avro-name)
  (aliases #() array[avro-fullname] t)
  (doc "" string-schema t)
  (type error avro-schema)
  (order "ascending" enum[ordering] t)
  (default nil t t)
  (with-assertions
    (declare (inline assert-valid-default))
    (when defaultp
      (assert-valid-default type default))))

(defmethod assert-valid ((schema field-schema) object)
  (declare (optimize (speed 3) (safety 0)))
  (assert-valid (field-schema-type schema) object))

(defset field-schema (#'equal #'field-schema-name))

(defcomplex record-schema (named doc)
  "Represents an avro record schema."
  (fields error set[field-schema]))

(defmethod assert-valid ((schema record-schema) (hash-table hash-table))
  (declare (optimize (speed 3) (safety 0)))
  (let ((test (hash-table-test hash-table)))
    (unless (eq test 'equal)
      (error "Hash-table's test is ~S, not #'equal" test)))
  (let ((fields (record-schema-fields schema)))
    (let ((expected (length fields))
          (actual (hash-table-count hash-table)))
      (unless (= expected actual)
        (error "Hash-table has ~S elements, not ~S" actual expected)))
    (flet ((assert-valid (field)
             (let ((name (field-schema-name field))
                   (type (field-schema-type field)))
               (multiple-value-bind (value valuep)
                   (gethash name hash-table)
                 (unless valuep
                   (error "Field ~S not found in hash-table" name))
                 (assert-valid type value)))))
      (map nil #'assert-valid fields))))

;;; avro logical schemas

(defmacro deflogical ((name underlying-type) &body slots)
  "Define a logical schema named NAME.

UNDERLYING-TYPE overrides the type of LOGICAL-SCHEMA's
UNDERLYING-SCHEMA type.

SLOTS must adhere to DEFSTRUCT+."
  (declare (symbol name)
           ((or symbol cons) underlying-type))
  `(defstruct+ (,name logical-schema
                      (underlying-schema error ,underlying-type))
     ,@slots))

;; decimal-schema

(defun! get-max-precision (size)
    (((integer 1)) (integer 1))
  (let ((integer-to-log (1- (expt 2 (1- (* 8 size))))))
    (truncate (log integer-to-log 10))))

(defun! %assert-decent-precision (precision size)
    (((integer 1) (integer 1)) (values))
  (declare (inline get-max-precision))
  (let ((max-precision (get-max-precision size)))
    (when (> precision max-precision)
      (error "fixed-schema with size ~S can store up to ~S digits, not ~S"
             size
             max-precision
             precision))))

(defun! assert-decent-precision (precision size)
    (((integer 1) (integer 0)) (values))
  (declare (inline %assert-decent-precision))
  (when (zerop size)
    (error "fixed-schema has size 0"))
  (%assert-decent-precision precision size))

(deflogical (decimal-schema
             (or (eql bytes-schema) fixed-schema))
  "Represents an avro decimal schema."
  (scale 0 (integer 0))
  (precision error (integer 1))
  (with-assertions
    ;; TODO bug when inlining
    ;; (declare (inline assert-decent-precision))
    (unless (<= scale precision)
      (error "Scale ~S cannot be greater than precision ~S" scale precision))
    (when (fixed-schema-p underlying-schema)
      (assert-decent-precision precision (fixed-schema-size underlying-schema)))))

(defun! number-of-digits (integer)
    ((integer) (integer 1))
  (if (zerop integer)
      1
      (let ((abs (abs integer)))
        (ceiling (log (1+ abs) 10)))))

(defmethod assert-valid ((schema decimal-schema) (unscaled integer))
  (declare (inline number-of-digits)
           (optimize (speed 3) (safety 0)))
  (let ((number-of-digits (number-of-digits unscaled))
        (max-precision (decimal-schema-precision schema)))
    (unless (<= number-of-digits max-precision)
      (error "Decimal schema with ~S precision cannot represent ~S digits"
             max-precision
             number-of-digits))))

;; duration-schema

(deflogical (duration-schema fixed-schema)
  "Represents an avro duration schema."
  (with-assertions
    (let ((size (fixed-schema-size underlying-schema)))
      (unless (= 12 size)
        (error "Size of fixed-schema must be 12, not ~S" size)))))

(defstruct+ (duration)
  "Represents a duration."
  (months 0 (unsigned-byte 32))
  (days 0 (unsigned-byte 32))
  (milliseconds 0 (unsigned-byte 32)))
