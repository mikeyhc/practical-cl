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
