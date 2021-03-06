(in-package :cl-user)

(defpackage :net.atmosia.mp3-database
  (:use :common-lisp
        :net.atmosia.pathnames
        :net.atmosia.macro-utilities
        :net.atmosia.id3v2)
  (:export :*default-table-size*
           :*mp3-schema*
           :*mp3s*
           :column
           :column-value
           :delete-all-rows
           :delete-rows
           :do-rows
           :extract-schema
           :in
           :insert-row
           :load-database
           :make-column
           :make-schema
           :map-rows
           :matching
           :not-nullable
           :nth-row
           :random-selection
           :schema
           :select
           :shuffle-table
           :sort-rows
           :table
           :table-size
           :with-column-values))

(in-package :net.atmosia.mp3-database)

(defparameter *default-table-size* 100)

(defun make-rows (&optional (size *default-table-size*))
  (make-array size :adjustable t :fill-pointer 0))

(defclass table ()
  ((rows   :accessor rows   :initarg :rows :initform (make-rows))
   (schema :accessor schema :initarg :schema)))

(defclass column ()
  ((name
     :reader name
     :initarg :name)

   (equality-predicate
     :reader equality-predicate
     :initarg :equality-predicate)

   (comparator
     :reader comparator
     :initarg :comparator)

   (default-value
     :reader default-value
     :initarg :default-value
     :initform nil)

   (value-normalizer
     :reader value-normalizer
     :initarg :value-normalizer
     :initform #'(lambda (v column) (declare (ignore column)) v))))

(defun intern-for-column (value column)
  (let ((hash (interned-values column)))
    (or (gethash (not-nullable value column) hash)
        (setf (gethash value hash) value))))

(defclass interned-values-column (column)
  ((interned-values
     :reader interned-values
     :initform (make-hash-table :test #'equal))
   (equality-predicate :initform #'eql)
   (value-normalizer   :initform #'intern-for-column)))

(defgeneric make-column (name type &optional default-value))

(defun not-nullable (value column)
  (or value (error "Column ~a can't be null" (name column))))

(defmethod make-column (name (type (eql 'string)) &optional default-value)
  (make-instance
    'column
    :name name
    :comparator #'string<
    :equality-predicate #'string=
    :default-value default-value
    :value-normalizer #'not-nullable))

(defmethod make-column (name (type (eql 'number)) &optional default-value)
  (make-instance
    'column
    :name name
    :comparator #'<
    :equality-predicate #'=
    :default-value default-value))

(defmethod make-column (name (type (eql 'interned-string))
                        &optional default-value)
  (make-instance
    'interned-values-column
    :name name
    :comparator #'string<
    :default-value default-value))

(defun make-schema (spec)
  (mapcar #'(lambda (column-spec) (apply #'make-column column-spec)) spec))

(defparameter *mp3-schema*
  (make-schema
    '((:file     string)
      (:genre    interned-string "Unknown")
      (:artist   interned-string "Unknown")
      (:album    interned-string "Unknown")
      (:song     string)
      (:track    number 0)
      (:year     number 0)
      (:id3-size number))))

(defparameter *mp3s* (make-instance 'table :schema *mp3-schema*))

(defun normalize-for-column (value column)
  (funcall (value-normalizer column) value column))

(defun normalize-row (names-and-values schema)
  (loop
    for column in schema
    for name  = (name column)
    for value = (or (getf names-and-values name) (default-value column))
    collect name
    collect (normalize-for-column value column)))

(defun insert-row (names-and-values table)
  (vector-push-extend (normalize-row names-and-values (schema table))
                      (rows table)))

(defun parse-track (track)
  (when  track (parse-integer track :end (position #\/ track))))

(defun parse-year (year)
  (when year (parse-integer year)))

(defun file->row (file)
  (let ((id3 (read-id3 file)))
    (list
      :file     (namestring (truename file))
      :genre    (translated-genre id3)
      :artist   (artist id3)
      :song     (song id3)
      :track    (parse-track (track id3))
      :year     (parse-year (year id3))
      :id3-size (size id3))))

(defun load-database (dir db)
  (let ((count 0))
    (walk-directory
      dir
      #'(lambda (file)
          (princ #\.)
          (incf count)
          (insert-row (file->row file) db))
      :test #'mp3-p)
    (format t "~&Loaded ~d files into database." count)))

(defun mklist (thing)
  (if (listp thing) thing (list thing)))

(defun find-column (column-name schema)
  (or (find column-name schema :key #'name)
      (error "No column: `~a in schema: ~a" column-name schema)))

(defun extract-schema (column-names schema)
  (loop for c in column-names collect (find-column c schema)))

(defun restrict-rows (rows where)
  (remove-if-not where rows))

(defun extractor (schema)
  (let ((names (mapcar #'name schema)))
    #'(lambda (row)
        (loop for c in names collect c collect (getf row c)))))

(defun project-columns (rows schema)
  (map 'vector (extractor schema) rows))

(defun row-equality-tester (schema)
  (let ((names (mapcar #'name schema))
        (tests (mapcar #'equality-predicate schema)))
    #'(lambda (a b)
        (loop for name in names and test in tests
              always (funcall test (getf a name) (getf b name))))))

(defun distinct-rows (rows schema)
  (remove-duplicates rows :test (row-equality-tester schema)))

(defun row-comparator (column-names schema)
  (let ((comparators (mapcar #'comparator
                             (extract-schema column-names schema))))
    #'(lambda (a b)
        (loop
          for name in column-names
          for comparator in comparators
          for a-value = (getf a name)
          for b-value = (getf b name)
          when (funcall comparator a-value b-value) return t
          when (funcall comparator b-value a-value) return nil
          finally (return nil)))))

(defun sorted-rows (rows schema order-by)
  (sort (copy-seq rows) (row-comparator order-by schema)))

(defun select (&key (columns t) from where distinct order-by)
  (let ((rows (rows from))
        (schema (schema from)))
    (when where
      (setf rows (restrict-rows rows where)))
    (unless (eql columns 't)
      (setf schema (extract-schema (mklist columns) schema))
      (setf rows (project-columns rows schema)))
    (when distinct
      (setf rows (distinct-rows rows schema)))
    (when order-by
      (setf rows (sorted-rows rows schema (mklist order-by))))
    (make-instance 'table :rows rows :schema schema)))

(defun column-matcher (column value)
  (let ((name (name column))
        (predicate (equality-predicate column))
        (normalized (normalize-for-column value column)))
    #'(lambda (row) (funcall predicate (getf row name) normalized))))

(defun column-matchers (schema names-and-values)
  (loop for (name value) on names-and-values by #'cddr
        when value collect
        (column-matcher (find-column name schema) value)))

(defun matching (table &rest names-and-values)
  "Build a where function that matches rows with the given column values."
  (let ((matchers (column-matchers (schema table) names-and-values)))
    #'(lambda (row)
        (every #'(lambda (matcher) (funcall matcher row)) matchers))))

(defun in (column-name table)
  (let ((test (equality-predicate (find-column column-name (schema table))))
        (values (map 'list #'(lambda (r) (getf r column-name)) (rows table))))
    #'(lambda (row)
        (member (getf row column-name) values :test test))))

(defmacro do-rows ((row table) &body body)
  `(loop for ,row across (rows ,table) do ,@body))

(defun map-rows (fn table)
  (loop for row in (rows table) collect (funcall fn row)))

(defun column-value (row column-name)
  (getf row column-name))

(defun as-keyword (symbol)
  (intern (symbol-name symbol) :keyword))

(defun column-bindings (vars row)
  (loop for v in vars collect `(,v (column-value ,row ,(as-keyword v)))))

(defmacro with-column-values ((&rest vars) row &body body)
  (once-only (row)
    `(let ,(column-bindings vars row) ,@body)))

(defun table-size (table)
  (length (rows table)))

(defun nth-row (n table)
  (aref (rows table) n))

(defun delete-rows (&key from where)
  (loop
    with rows = (rows from)
    with store-idx = 0
    for read-idx from 0
    for row across rows
    do (setf (aref rows read-idx) nil)
    unless (funcall where row) do
      (setf (aref rows store-idx) row)
      (incf store-idx)
   finally (setf (fill-pointer rows) store-idx)))

(defun delete-all-rows (table)
  (setf (rows table) (make-rows *default-table-size*)))

(defun sort-rows (table &rest column-names)
  (setf (rows table) (sort (rows table)
                           (row-comparator column-names (schema table))))
  table)

(defun nshuffle-vector (vector)
  (loop for idx downfrom (1- (length vector)) to 1
        for other = (random (1+ idx))
        do (unless (= idx other)
             (rotatef (aref vector idx) (aref vector other)))))

(defun shuffle-table (table)
  (nshuffle-vector (rows table)))

(defun random-sample (vector n)
  "Based on Algorithm S from Knuth. TAOCP, vol. 2. p. 142"
  (loop with selected = (make-array n :fill-pointer 0)
        for idx from 0
        do
         (loop
           with to-select = (- n (length selected))
           for remaining = (- (length vector) idx)
           while (>= (* remaining (random 1.0)) to-select)
           do (incf idx))
         (vector-push (aref vector idx) selected)
         when (= (length selected) n) return selected))

(defun random-selection (table n)
  (make-instance
    'table
    :schema (schema table)
    :rows (nshuffle-vector (random-sample (rows table) n))))
