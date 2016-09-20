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
           :extract-scheme
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

(defclass interned-values-column (column)
  ((interned-values
     :reader interned-values
     :initform (make-hash-table :test #'equal))
   (equality-predicate :initform #'eql)
   (value-normalizer   :initform #'intern-for-column)))

(defgeneric make-column (name type &optional default-value))

(defun not-nullable (value column)
  (or value (error "Column ~a can't be null" (name column))))

(defun intern-for-column (value column)
  (let ((hash (interned-values column)))
    (or (gethash (not-nullable value column) hash)
        (setf (gethash value hash) value))))

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
