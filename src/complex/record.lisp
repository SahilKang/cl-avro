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
(defpackage #:cl-avro.internal.record
  (:use #:cl)
  (:local-nicknames
   (#:api #:cl-avro)
   (#:internal #:cl-avro.internal)
   (#:mop #:cl-avro.internal.mop)
   (#:name #:cl-avro.internal.name))
  (:import-from #:cl-avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>
                #:comparison)
  (:import-from #:cl-avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:export #:read-field))
(in-package #:cl-avro.internal.record)

;;; field

;; TODO export this
(deftype order ()
  '(member api:ascending api:descending api:ignore))

(deftype alias ()
  'name:name)

(deftype array<alias> ()
  '(simple-array alias (*)))

(deftype array<alias>? ()
  '(or null array<alias>))

(defclass api:field (closer-mop:standard-direct-slot-definition)
  ((aliases
    :initarg :aliases
    :reader api:aliases
    :type array<alias>?
    :documentation "A vector of aliases if provided, otherwise nil.")
   (order
    :initarg :order
    :type order
    :documentation "Field ordering used during sorting.")
   (default
    :initarg :default
    :type t
    :documentation "Field default."))
  (:default-initargs
   :name (error "Must supply NAME")
   :type (error "Must supply TYPE"))
  (:documentation
   "Slot class for an avro record field."))

(defmethod api:order
    ((instance api:field))
  "Return (values order provided-p)."
  (let* ((orderp (slot-boundp instance 'order))
         (order (if orderp
                    (slot-value instance 'order)
                    'api:ascending)))
    (values order orderp)))

(defmethod api:default
    ((instance api:field))
  "Return (values default provided-p)."
  (let* ((defaultp (slot-boundp instance 'default))
         (default (when defaultp
                    (slot-value instance 'default))))
    (values default defaultp)))

(defmethod api:name
    ((instance api:field))
  "Return (values name-string slot-symbol)."
  (let ((name (closer-mop:slot-definition-name instance)))
    (declare (symbol name))
    (values (string name) name)))

(defmethod api:type
    ((instance api:field))
  "Field type."
  (closer-mop:slot-definition-type instance))

(declaim (ftype (function (t) (values alias &optional)) parse-alias))
(defun parse-alias (alias)
  (check-type alias alias)
  alias)

(declaim
 (ftype (function (sequence boolean) (values array<alias>? &optional))
        parse-aliases))
(defun parse-aliases (aliases aliasesp)
  (when aliasesp
    (assert (null (internal:duplicates aliases)) (aliases))
    (map 'array<alias> #'parse-alias aliases)))

(declaim
 (ftype (function (string) (values order &optional)) %parse-order))
(defun %parse-order (order)
  (let ((expected '("ascending" "descending" "ignore")))
    (assert (member order expected :test #'string=) (expected)))
  (nth-value 0 (find-symbol (string-upcase order) 'cl-avro)))

(declaim (ftype (function (t) (values order &optional)) parse-order))
(defun parse-order (order)
  (etypecase order
    (order order)
    (string (%parse-order order))))

(defmethod initialize-instance :around
    ((instance api:field)
     &rest initargs &key (aliases nil aliasesp) (order 'api:ascending orderp))
  (setf (getf initargs :aliases) (parse-aliases aliases aliasesp))
  (when orderp
    (setf (getf initargs :order) (parse-order order)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance api:field) &key)
  (with-accessors
        ((name closer-mop:slot-definition-name)
         (initfunction closer-mop:slot-definition-initfunction)
         (initform closer-mop:slot-definition-initform)
         (allocation closer-mop:slot-definition-allocation)
         (type closer-mop:slot-definition-type))
      instance
    (check-type type (or api:schema symbol))
    (assert (typep (string name) 'name:name))
    (assert (null initfunction) ()
            "Did not expect an initform for slot ~S: ~S" name initform)
    (assert (eq allocation :instance) ()
            "Expected :INSTANCE allocation for slot ~S, not ~S"
            name allocation)))

;;; effective-field

(defclass effective-field
    (api:field closer-mop:standard-effective-slot-definition)
  ((default :type api:object)))

(defmethod initialize-instance :around
    ((instance effective-field)
     &rest initargs &key type (default nil defaultp))
  (when (and (symbolp type)
             (not (typep type 'api:schema)))
    (let ((schema (find-class type)))
      (setf type schema
            (getf initargs :type) schema)))
  (when defaultp
    (setf (getf initargs :default)
          (internal:deserialize-field-default type default)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance effective-field) &key)
  (let ((type (api:type instance)))
    (check-type type api:schema)))

(declaim
 (ftype (function (effective-field t) (values &optional)) set-default-once))
(defun set-default-once (field default)
  (unless (slot-boundp field 'default)
    (setf (slot-value field 'default)
          (internal:deserialize-field-default (api:type field) default)))
  (values))

(declaim
 (ftype (function (effective-field order) (values &optional)) set-order-once))
(defun set-order-once (field order)
  (unless (slot-boundp field 'order)
    (setf (slot-value field 'order) order))
  (values))

(declaim
 (ftype (function (effective-field array<alias>) (values &optional))
        set-aliases-once))
(defun set-aliases-once (field aliases)
  ;; aliases slot is always bound
  (unless (slot-value field 'aliases)
    (setf (slot-value field 'aliases) aliases))
  (values))

;;; record

(deftype fields ()
  '(simple-array api:field (*)))

(defclass api:record (name:named-schema)
  ((fields
    :type fields
    :documentation "Record fields."))
  (:metaclass mop:schema-class)
  (:object-class api:record-object)
  (:documentation
   "Metaclass of avro record schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:record) (superclass name:named-schema))
  t)

(defmethod closer-mop:direct-slot-definition-class
    ((class api:record) &key)
  (find-class 'api:field))

(defmethod closer-mop:effective-slot-definition-class
    ((class api:record) &key)
  (find-class 'effective-field))

(defmethod closer-mop:compute-effective-slot-definition
    ((class api:record) name slots)
  (let ((effective-slot (call-next-method)))
    (dolist (slot slots effective-slot)
      (multiple-value-bind (default defaultp)
          (api:default slot)
        (when defaultp
          (set-default-once effective-slot default)))
      (multiple-value-bind (order orderp)
          (api:order slot)
        (when orderp
          (set-order-once effective-slot order)))
      (let ((aliases (api:aliases slot)))
        (when aliases
          (set-aliases-once effective-slot aliases))))))

(defmethod (setf closer-mop:slot-value-using-class)
    (new-value (class api:record) object (slot api:field))
  (let ((type (api:type slot))
        (name (api:name slot)))
    (assert (typep new-value type) (new-value)
            "Expected type ~S for field ~S, but got type ~S instead: ~S"
            type name (type-of new-value) new-value))
  (call-next-method))

(defmethod api:fields
    ((instance api:record))
  (closer-mop:ensure-finalized instance)
  (slot-value instance 'fields))

(declaim (ftype (function (list) (values list &optional)) add-initargs))
(defun add-initargs (slots)
  (flet ((add-initarg (slot)
           (let* ((name (string (getf slot :name)))
                  (initarg (intern name 'keyword)))
             (pushnew initarg (getf slot :initargs)))
           (flet ((add-initarg (alias)
                    (pushnew (intern alias 'keyword)
                             (getf slot :initargs))))
             (map nil #'add-initarg (getf slot :aliases)))
           slot))
    (mapcar #'add-initarg slots)))

(declaim (ftype (function (cons) (values keyword &optional)) lispiest-initarg))
(defun lispiest-initarg (slot)
  (flet ((initarg< (initarg1 initarg2)
           (declare (keyword initarg1 initarg2))
           (let ((initarg1 (string initarg1))
                 (initarg2 (string initarg2)))
             (and (string/= initarg1 initarg2)
                  (string= (remove #\- initarg1 :test #'char=)
                           (remove #\_ (string-upcase initarg2) :test #'char=))))))
    (first (sort (copy-list (getf slot :initargs)) #'initarg<))))

(declaim
 (ftype (function (list list) (values list &optional)) add-default-initargs))
(defun add-default-initargs (slots default-initargs)
  (labels
      ((preexisting (slot)
         (loop
           for initarg in (getf slot :initargs)
           when (find initarg default-initargs :test #'eq :key #'first)
             return it))
       (preexisting-or-new (slot)
         (let* ((name (string (getf slot :name)))
                (initarg (lispiest-initarg slot))
                (error-form `(error ,(format nil "Must supply ~A" name)))
                (lambda-form `(lambda () ,error-form)))
           (or (preexisting slot)
               `(,initarg ,error-form ,(compile nil lambda-form))))))
    (mapcar #'preexisting-or-new slots)))

(mop:definit ((instance api:record) :around
              &rest initargs &key direct-slots direct-default-initargs)
  (setf (getf initargs :direct-slots) (add-initargs direct-slots)
        (getf initargs :direct-default-initargs) (add-default-initargs
                                                  (getf initargs :direct-slots)
                                                  direct-default-initargs))
  (apply #'call-next-method instance initargs))

(deftype slot-names ()
  '(simple-array symbol (*)))

(declaim
 (ftype (function (api:record) (values slot-names &optional))
        ordered-slot-names))
(defun ordered-slot-names (instance)
  (let* ((slots (closer-mop:class-direct-slots instance))
         (names (mapcar #'closer-mop:slot-definition-name slots)))
    (make-array (length slots) :element-type 'symbol :initial-contents names)))

(declaim
 (ftype (function (list slot-names) (values fields &optional)) sort-slots))
(defun sort-slots (slots ordered-slot-names)
  (flet ((slot-position (slot)
           (let ((name (closer-mop:slot-definition-name slot)))
             (position name ordered-slot-names))))
    (let ((slots (make-array (length slots)
                             :element-type 'effective-field
                             :initial-contents slots)))
      (sort slots #'< :key #'slot-position))))

(defmethod closer-mop:finalize-inheritance :after
    ((instance api:record))
  (with-slots (fields) instance
    (let ((ordered-slot-names (ordered-slot-names instance))
          (slots (closer-mop:compute-slots instance)))
      (setf fields (sort-slots slots ordered-slot-names)))))

;;; record-object

(defclass api:record-object (api:complex-object)
  ()
  (:documentation
   "Base class for instances of an avro record schema."))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:record))
  (let* ((fields (api:fields schema))
         (types (map '(simple-array api:schema (*)) #'api:type fields))
         (sizes (map 'list #'internal:fixed-size types)))
    (declare (fields fields))
    (when (and (every #'identity sizes)
               (apply #'= sizes))
      (first sizes))))

(defmethod api:serialized-size
    ((object api:record-object))
  (flet ((add-size (total field)
           (let ((value (slot-value object (nth-value 1 (api:name field)))))
             (+ total (api:serialized-size value)))))
    (let ((fields (api:fields (class-of object))))
      (declare (fields fields))
      (reduce #'add-size fields :initial-value 0))))

;;; serialize

(defmethod internal:serialize
    ((object api:record-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields (class-of object))
    and bytes-written of-type ufixnum = 0

    for field across fields
    for value of-type api:object = (slot-value
                                    object (nth-value 1 (api:name field)))

    do (incf bytes-written
             (internal:serialize value into :start (+ start bytes-written)))

    finally
       (return
         bytes-written)))

(defmethod internal:serialize
    ((object api:record-object) (into stream) &key)
  (loop
    with fields of-type fields = (api:fields (class-of object))
    and bytes-written of-type ufixnum = 0

    for field across fields
    for value of-type api:object = (slot-value
                                    object (nth-value 1 (api:name field)))

    do (incf bytes-written (internal:serialize value into))

    finally
       (return
         bytes-written)))

(defmethod api:serialize
    ((object api:record-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object)) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema api:record) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields schema)
    and initargs of-type list = nil
    and total-bytes-read of-type ufixnum = 0

    for field across fields
    for new-start of-type ufixnum = (+ start total-bytes-read)
    for value of-type api:object
      = (multiple-value-bind (value bytes-read)
            (api:deserialize (api:type field) input :start new-start)
          (declare (ufixnum bytes-read))
          (incf total-bytes-read bytes-read)
          value)

    do
       (push value initargs)
       ;; TODO do the thing below
       (push (lispiest-initarg
              (list :initargs (closer-mop:slot-definition-initargs field)))
             initargs)

    finally
       (return
         (values (apply #'make-instance schema initargs) total-bytes-read))))

(defmethod api:deserialize
    ((schema api:record) (input stream) &key)
  (loop
    with fields of-type fields = (api:fields schema)
    and initargs of-type list = nil
    and total-bytes-read of-type ufixnum = 0

    for field across fields
    for value of-type api:object
      = (multiple-value-bind (value bytes-read)
            (api:deserialize (api:type field) input)
          (declare (ufixnum bytes-read))
          (incf total-bytes-read bytes-read)
          value)

    do
       (push value initargs)
       ;; TODO do the thing below
       (push (lispiest-initarg
              (list :initargs (closer-mop:slot-definition-initargs field)))
             initargs)

    finally
       (return
         ;; TODO need to coalesce upper/lower keyword initargs with
         ;; an around method on initialize-instance, or use the
         ;; lispiest initarg when building initargs.  is add-initarg
         ;; really a good thing then?
         ;; or just remove the default-initargs stuff and have a restart
         ;; catch unset slots
         ;; or do none of that and just have slot-value assert the value
         (values (apply #'make-instance schema initargs) total-bytes-read))))

;;; compare

(defmethod internal:skip
    ((schema api:record) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields schema)
    and bytes-read of-type ufixnum = 0

    for field across fields
    for type of-type api:schema = (api:type field)
    for new-start of-type ufixnum = (+ start bytes-read)

    do (incf bytes-read (internal:skip type input new-start))

    finally
       (return
         bytes-read)))

(defmethod internal:skip
    ((schema api:record) (input stream) &optional start)
  (declare (ignore start))
  (loop
    with fields of-type fields = (api:fields schema)
    and bytes-read of-type ufixnum = 0

    for field across fields
    for type of-type api:schema = (api:type field)

    do (incf bytes-read (internal:skip type input))

    finally
       (return
         bytes-read)))

(defmethod api:compare
    ((schema api:record) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (loop
    with fields of-type fields = (api:fields schema)
    and left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for field across fields
    for order of-type order = (api:order field)
    for type of-type api:schema = (api:type field)
    for left-new-start of-type ufixnum = (+ left-start left-bytes-read)
    for right-new-start of-type ufixnum = (+ right-start right-bytes-read)

    if (eq order 'api:ignore) do
      (incf left-bytes-read (internal:skip type left left-new-start))
      (incf right-bytes-read (internal:skip type right right-new-start))
    else do
      (multiple-value-bind (comparison left right)
          (api:compare
           type left right
           :left-start left-new-start :right-start right-new-start)
        (declare (comparison comparison)
                 (ufixnum left right))
        (incf left-bytes-read left)
        (incf right-bytes-read right)
        (unless (zerop comparison)
          (return-from api:compare
            (values (if (eq order 'api:ascending)
                        comparison
                        (- comparison))
                    left-bytes-read
                    right-bytes-read))))

    finally
       (return
         (values 0 left-bytes-read right-bytes-read))))

(defmethod api:compare
    ((schema api:record) (left stream) (right stream) &key)
  (loop
    with fields of-type fields = (api:fields schema)
    and left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for field across fields
    for order of-type order = (api:order field)
    for type of-type api:schema = (api:type field)

    if (eq order 'api:ignore) do
      (incf left-bytes-read (internal:skip type left))
      (incf right-bytes-read (internal:skip type right))
    else do
      (multiple-value-bind (comparison left right)
          (api:compare type left right)
        (declare (comparison comparison)
                 (ufixnum left right))
        (incf left-bytes-read left)
        (incf right-bytes-read right)
        (unless (zerop comparison)
          (return-from api:compare
            (values (if (eq order 'api:ascending)
                        comparison
                        (- comparison))
                    left-bytes-read
                    right-bytes-read))))

    finally
       (return
         (values 0 left-bytes-read right-bytes-read))))

;;; coerce

(declaim
 (ftype (function (name:name fields) (values (or null api:field) &optional))
        find-field-by-name))
(defun find-field-by-name (name fields)
  (loop
    for field across fields
    for field-name of-type name:name = (api:name field)
    when (string= name field-name)
      return field))

(declaim
 (ftype (function (array<alias>? fields)
                  (values (or null api:field) &optional))
        find-field-by-alias))
(defun find-field-by-alias (aliases fields)
  (when aliases
    (loop
      for field across fields
      for field-name of-type name:name = (api:name field)
      when (find field-name aliases :test #'string=)
        return field)))

(declaim
 (ftype (function (api:field fields) (values (or null api:field) &optional))
        find-field))
(defun find-field (field fields)
  (or (find-field-by-name (api:name field) fields)
      (find-field-by-alias (api:aliases field) fields)))

(defmethod api:coerce
    ((object api:record-object) (schema api:record))
  (if (eq (class-of object) schema)
      object
      (loop
        with writer of-type api:schema = (class-of object)
        with writer-fields of-type fields = (api:fields writer)
        and reader-fields of-type fields = (api:fields schema)
        and initargs of-type list = nil

              initially
                 (name:assert-matching-names schema writer)

        for reader-field across reader-fields
        for initarg of-type keyword = (intern (api:name reader-field) 'keyword)
        for writer-field of-type (or null api:field)
          = (find-field reader-field writer-fields)

        if writer-field do
          (let* ((writer-slot-name (nth-value 1 (api:name writer-field)))
                 (writer-value (slot-value object writer-slot-name))
                 (reader-type (api:type reader-field)))
            (declare (symbol writer-slot-name)
                     (api:object writer-value)
                     (api:schema reader-type))
            (push (api:coerce writer-value reader-type) initargs)
            (push initarg initargs))
        else do
          (multiple-value-bind (default defaultp)
              (api:default reader-field)
            (declare (api:object default)
                     (boolean defaultp))
            (unless defaultp
              (error "Writer field ~S does not exist and reader has no default"
                     (api:name reader-field)))
            (push default initargs)
            (push initarg initargs))

        finally
           (return
             (apply #'change-class object schema initargs)))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:record-object))
  (let ((jso (st-json:jso)))
    (flet ((fill-jso (field)
             (multiple-value-bind (name slot-name)
                 (api:name field)
               (setf (st-json:getjso name jso)
                     (internal:serialize-field-default
                      (slot-value default slot-name))))))
      (map nil #'fill-jso (api:fields (class-of default))))
    jso))

(declaim
 (ftype (function (api:record) (values hash-table &optional)) name->type))
(defun name->type (schema)
  (loop
    with fields of-type fields = (api:fields schema)
    with name->type = (make-hash-table :test #'equal :size (length fields))

    for field across fields
    for name of-type name:name = (api:name field)
    for type of-type api:schema = (api:type field)

    do (setf (gethash name name->type) type)

    finally
       (return
         name->type)))

(defmethod internal:deserialize-field-default
    ((schema api:record) (default st-json:jso))
  (let ((name->type (name->type schema))
        initargs)
    (flet ((fill-initargs (key value)
             (let* ((type (gethash key name->type))
                    (value (internal:deserialize-field-default type value))
                    (key (intern key 'keyword)))
               (push value initargs)
               (push key initargs))))
      (st-json:mapjso #'fill-initargs default))
    (apply #'make-instance schema initargs)))

(defmethod internal:deserialize-field-default
    ((schema api:record) (default list))
  (let ((initargs (if (consp (first default))
                      (alist->initargs default)
                      (plist->initargs default))))
    (apply #'make-instance schema initargs)))

(declaim (ftype (function (cons) (values cons &optional)) alist->initargs))
(defun alist->initargs (alist)
  (loop
    for (key . value) in alist
    collect (intern (string key) 'keyword)
    collect value))

(declaim (ftype (function (list) (values list &optional)) plist->initargs))
(defun plist->initargs (plist)
  (loop
    for remaining = plist then (cddr remaining)
    while remaining

    for key = (car remaining)
    for rest = (cdr remaining)
    for value = (car rest)

    unless rest do
      (error "Odd number of key-value pairs: ~S" plist)

    collect (intern (string key) 'keyword)
    collect value))

;;; jso

(declaim
 (ftype (function (st-json:jso hash-table name:namespace)
                  (values list &optional))
        read-field))
(defun read-field (jso fullname->schema enclosing-namespace)
  (internal:with-initargs ((doc :documentation) order aliases default) jso
    (multiple-value-bind (name namep)
        (st-json:getjso "name" jso)
      (assert namep () "Field name must be provided.")
      (push (make-symbol name) initargs)
      (push :name initargs))
    (multiple-value-bind (type typep)
        (st-json:getjso "type" jso)
      (assert typep () "Field type must be provided.")
      (push (internal:read-jso type fullname->schema enclosing-namespace) initargs)
      (push :type initargs))
    initargs))

(declaim
 (ftype (function (st-json:jso api:record hash-table) (values list &optional))
        read-fields))
(defun read-fields (jso record fullname->schema)
  (let ((namespace (api:namespace record)))
    (flet ((read-field (jso)
             (read-field jso fullname->schema namespace)))
      (multiple-value-bind (fields fieldsp)
          (st-json:getjso "fields" jso)
        (assert fieldsp () "Record schema must provide an array of fields.")
        (mapcar #'read-field fields)))))

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "record")) fullname->schema enclosing-namespace)
      (internal:with-initargs (name namespace aliases (doc :documentation)) jso
        (push enclosing-namespace initargs)
        (push :enclosing-namespace initargs)
        (let* ((schema (apply #'make-instance 'api:record initargs))
               (fullname (api:fullname schema)))
          (assert (not (gethash fullname fullname->schema)) ()
                  "Name ~S is already taken" fullname)
          (setf (gethash fullname fullname->schema) schema
                (getf initargs :direct-slots) (read-fields jso schema fullname->schema))
          (apply #'reinitialize-instance schema initargs)))))

(defmethod internal:write-jso
    ((field api:field) seen canonical-form-p)
  (let ((initargs (list
                   "name" (api:name field)
                   "type" (internal:write-jso (api:type field) seen canonical-form-p)))
        (aliases (api:aliases field))
        (documentation (documentation field t)))
    (unless canonical-form-p
      (when aliases
        (push aliases initargs)
        (push "aliases" initargs))
      (when documentation
        (push documentation initargs)
        (push "doc" initargs))
      (multiple-value-bind (order orderp)
          (api:order field)
        (when orderp
          ;; TODO move this downcase-symbol into api:order method
          (push (internal:downcase-symbol order) initargs)
          (push "order" initargs)))
      (multiple-value-bind (default defaultp)
          (api:default field)
        (when defaultp
          (push (internal:serialize-field-default default) initargs)
          (push "default" initargs))))
    (apply #'st-json:jso initargs)))

(declaim
 (ftype (function (api:record hash-table boolean)
                  (values (simple-array st-json:jso (*)) &optional))
        write-fields))
(defun write-fields (schema seen canonical-form-p)
  (flet ((write-field (field)
           (internal:write-jso field seen canonical-form-p)))
    (map '(simple-array st-json:jso (*)) #'write-field (api:fields schema))))

(defmethod internal:write-jso
    ((schema api:record) seen canonical-form-p)
  (let ((initargs (list "fields" (write-fields schema seen canonical-form-p)))
        (documentation (documentation schema t)))
    (unless canonical-form-p
      (when documentation
        (push documentation initargs)
        (push "doc" initargs)))
    initargs))
