(in-package :cl-user)

(defpackage :net.atmosia.mp3-browser
  (:use :common-lisp
        :net.aserve
        :net.atmosia.shoutcast
        :net.atmosia.mp3-database
        :net.atmosia.id3v2)
  (:import-from :acl-socket
                :ipaddr-to-dotted
                :remote-host)
  (:import-from :acl-compat.mp
                :make-process-lock
                :with-process-lock)
  (:export :start-mp3-browser))

(in-package :net.atmosia.mp3-browser)

(defun make-playlist-table ()
  (make-instance 'table :schema *mp3-schema*))

(defclass playlist ()
  ((id           :accessor id           :initarg :id)
   (songs-table  :accessor songs-table  :initform (make-playlist-table))
   (current-song :accessor current-song :initform *empty-playlist-song*)
   (current-idx  :accessor current-idx  :initform 0)
   (ordering     :accessor ordering     :initform :album)
   (shuffle      :accessor shuffle      :initform :none)
   (repeat       :accessor repeat       :initform :none)
   (user-agent   :accessor user-agent   :initform "Unknown")
   (lock         :reader   lock         :initform (make-process-lock))))

(defmacro with-playlist-locked ((playlist) &body body)
  `(with-process-lock ((lock ,playlist))
     ,@body))

(defvar *playlists* (make-hash-table :test #'equal))

(defparameter *playlists-lock* (make-process-lock :name "playlists-lock"))

(defparameter *silence-mp3* "mp3-browser/silence.mp3")

(defun lookup-playlist (id)
  (with-process-lock (*playlists-lock*)
    (or (gethash id *playlists*)
        (setf (gethash id *playlists*) (make-instance 'playlist :id 'id)))))

(defun playlist-id (request)
  (ipaddr-to-dotted (remote-host (request-socket request))))

(defmethod find-song-source ((type (eql 'playlist)) request)
  (let ((playlist (lookup-playlist (playlist-id request))))
    (with-playlist-locked (playlist)
      (let ((user-agent (header-slot-value request :user-agent)))
        (when user-agent (setf (user-agent playlist) user-agent))))
    playlist))

(defmethod current-song :around ((playlist playlist))
  (with-playlist-locked (playlist) (call-next-method)))

(defmethod still-current-p (song (playlist playlist))
  (with-playlist-locked (playlist)
    (eql song (current-song playlist))))

(defun at-end-p (playlist)
  (>= (current-idx playlist) (table-size (songs-table playlist))))

(defun file-for-current-idx (playlist)
  (if (at-end-p playlist)
    nil
    (column-value (nth-row (current-idx playlist) (songs-table playlist))
                  :file)))

(defun update-current-if-necessary (playlist)
  (unless (equal (file (current-song playlist))
                 (file-for-current-idx playlist))
    (reset-current-song playlist)))

(defun make-silent-song (title &optional (file *silence-mp3*))
  (make-instance
    'song
    :file file
    :title title
    :id3-size (if (id3-p file) (size (read-id3 file)) 0)))

(defparameter *empty-playlist-song*
  (make-silent-song "Playlist empty."))

(defparameter *end-of-playlist-song*
  (make-silent-song "At end of playlist."))

(defun row->song (song-db-entry)
  (with-column-values (file song artist id3-size) song-db-entry
    (make-instance
      'song
      :file file
      :title (format nil "~a by ~a from ~a" song artist album)
      :id3-size id3-size)))

(defun empty-p (playlist)
  (zerop (table-size (songs-table playlist))))

(defun reset-current-song (playlist)
  (setf
    (current-song playlist)
    (cond
      ((empty-p playlist) *empty-playlist-song*)
      ((at-end-p playlist) *end-of-playlist-song*)
      (t (row->song (nth-row (current-idx playlist)
                             (songs-table playlist)))))))

(defmethod maybe-move-to-next-song (song (playlist playlist))
  (with-playlist-locked (playlist)
    (when (still-current-p song playlist)
      (unless (at-end-p playlist)
        (ecase (repeat playlist)
          (:song) ; nothing changes
          (:none (incf (current-idx playlist)))
          (:all  (setf (current-idx playlist)
                       (mod (1+ (current-idx playlist))
                            (table-size (songs-table playlist)))))))
      (update-current-if-necessary playlist))))
