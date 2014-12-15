(in-package :cl-postgres-async)

;;;; Ad-hoc incoming message buffering

(defstruct message-buffer
  (data (fast-io:make-octet-vector fast-io:*default-output-buffer-size*)
   :type fast-io:octet-vector)
  (fill 0
   :type fast-io:index))

(defun push-chunk (buffer chunk)
  "Push an octet-vector CHUNK into message-buffer BUFFER"
  (declare #.*optimize*)
  (check-type buffer message-buffer)
  (check-type chunk fast-io:octet-vector)
  (let* ((fill (message-buffer-fill buffer))
	 (data (message-buffer-data buffer))
	 (advance (length chunk))
	 (size (length data))
	 (space (- size fill)))
    (when (< space advance)
      (setf (message-buffer-data buffer)
	    (setf data
		  (replace
		   (fast-io:make-octet-vector
		    (ash (+ size advance) 1))
		   data))))
    (replace data chunk :start1 fill)
    (incf (message-buffer-fill buffer) advance)))

(defun map-messages (buffer function)
  "Iterate over complete messages in BUFFER, calling FUNCTION for each
one, passing an input stream wherefrom the message can be read"
 ;(declare #.*optimize*)
  (let ((data (message-buffer-data buffer))
	(fill (message-buffer-fill buffer)))
    (fast-io:with-fast-input (buf data)
      (loop
	(let* ((head (the fast-io:index
			  (fast-io:buffer-position buf)))
	       (tail (- fill head)))
	  (flet ((short ()
		   (replace data data :start2 head :end2 fill)
		   (setf (message-buffer-fill buffer) tail)
		   (return)))
	    (when (< tail 5) (short))
	    (fast-io:fast-read-byte buf)
	    (let ((length (fast-io:readu32-be buf)))
	      (incf (fast-io::input-buffer-pos buf)
		    (- length 4))
	      (when (< tail (+ 1 length)) (short))
	      (pprint (subseq data head  (+ head 1 length)))
	      (funcall function
		       (flexi-streams:make-in-memory-input-stream
			data
			:start head
			:end (+ head 1 length))))))))))

(defclass async-database-connection (database-connection)
  ((buffer :initform (make-message-buffer))
   (callback :initform nil)))


(defmacro single-message-case (socket &body clauses)
  "Non-recursive variant of helper macro for reading messages from the
server. A list of cases \(characters that identify the message) can be
given, each with a body that handles the message, or the keyword :skip
to skip the message.  Cases for error and warning messages are always
added.

The body may contain an initial parameter of the form :LENGTH-SYM SYMBOL
where SYMBOL is a symbol to which the remaining length of the packet is
bound. This value indicates the number of bytes that have to be read
from the socket."
  (let ((socket-name (gensym))
        (size-name (gensym))
        (char-name (gensym))
        (iter-name (gensym))
        (t-found nil)
        (size-sym (and (eq (car clauses) :length-sym) (progn (pop clauses) (pop clauses)))))
    (flet ((expand-characters (chars)
             (cond ((eq chars t) (setf t-found t) t)
                   ((consp chars) (mapcar #'char-code chars))
                   (t (char-code chars)))))
      `(let* ((,socket-name ,socket))
	 (declare (type stream ,socket-name))
	 (labels ((,iter-name ()
		    (let ((,char-name (read-uint1 ,socket-name))
			  (,size-name (read-uint4 ,socket-name)))
		      (declare (type (unsigned-byte 8) ,char-name)
			       (type (unsigned-byte 32) ,size-name)
			       (ignorable ,size-name))
		      (case ,char-name
			(#.(char-code #\A)
			 (get-notification ,socket-name))
			(#.(char-code #\E) (get-error ,socket-name))
			(#.(char-code #\S) ;; ParameterStatus: read and continue
			 (update-parameter ,socket-name))
			(#.(char-code #\N) ;; A warning
			 (get-warning ,socket-name))
			,@(mapcar (lambda (clause)
				    `(,(expand-characters (first clause))
                                      ,(if (eq (second clause) :skip)
                                           `(skip-bytes ,socket-name (- ,size-name 4))
                                           (if size-sym
                                               `(let ((,size-sym (- ,size-name 4)))
                                                  ,@(cdr clause))
                                               `(progn ,@(cdr clause))))))
			   clauses)
			,@(unless t-found
			    `((t (ensure-socket-is-closed ,socket-name)
				 (error 'protocol-error
					:message (format nil "Unexpected message received: ~A"
							 (code-char ,char-name))))))))))
	   (,iter-name))))))


(defun async-handle-message (conn stream)
  (with-slots (callback) conn
    (format t "~&AHM: ~A~&" callback)
    (force-output)
    (funcall (or (shiftf callback nil)
		 (lambda (stream)
		   (single-message-case stream))) stream)))

(defun async-next-message (conn)
  (bb:with-promise (resolve reject)
    (setf (slot-value conn 'callback) #'resolve)))

(defun async-do-while (promise-fn test-fn)
  (bb:with-promise (resolve reject)
    (labels ((iter ()
	       (bb:aif (bb:attach (funcall promise-fn) test-fn)
		       (resolve)
		       (iter))))
      (iter))))

(defun async-initiate-connection (conn)
  (let ((buffer (slot-value conn 'buffer)))
    (setf (message-buffer-fill buffer) 0)
    (bb:with-promise (resolve reject)
      (bb:alet*
	  ((socket
	    (bb:with-promise (resolve reject)
	      (as:tcp-connect
	       (connection-host conn)
	       (connection-port conn)
	       (lambda (sock chunk)
		 (declare (ignorable sock))
		 (pprint chunk)
		 (push-chunk buffer chunk)
		 (map-messages buffer
			       (lambda (stream)
				 (async-handle-message conn stream))))
	       (lambda (ev)
		 (reject ev))
	       :connect-cb
	       (lambda (socket)
		 (setf (connection-socket conn)
		       (make-instance 'as:async-output-stream :socket socket))
		 (resolve socket)))))
	   (r
	    (startup-message (connection-socket conn)
			     (connection-user conn)
			     (connection-db conn)))
	   (nil
	    (progn
	      (pprint "After startup message")
	      (async-do-while
	       (lambda () (async-next-message conn))
	       (lambda (r)
		 (format t "~&Got stream ~A~&" r)
		 (let ((done nil))
		   (single-message-case r
		     (#\R
		      (let ((socket (connection-socket conn))
			    (password (connection-password conn))
			    (type (read-uint4 r)))
			(format t "~&Passwd ~A, req type ~A~&" password type)
			(ecase type
			  (0 (setf done t))
			  (3 (unless password
			       (error "Server requested plain-password authentication, but no password was given."))
			   (plain-password-message socket password)
			   (force-output socket)
			   (setf done t))
			  (5 (unless password
			       (error "Server requested md5-password authentication, but no password was given."))
			   (pprint `(type ,type end ,(flexi-streams::vector-stream-end r)
					  position ,(flexi-streams::file-position r)))

			   (md5-password-message socket password
						 (connection-user conn)
						 (read-bytes r 4))
			   (force-output socket))))))
		   (format t "Done: ~A~&" done)
		   done)))))
	   (nil
	    (progn
	      (pprint "After auth")
	      (async-do-while
	       (lambda () (async-next-message conn))
	       (lambda (r)
		 (let (done)
		   (single-message-case r
		     (#\K)
		     (#\Z
		      (setf done t)))
		   done))))))
	(resolve conn)))))

