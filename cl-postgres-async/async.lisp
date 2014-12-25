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
    (funcall (or (shiftf callback nil)
		 (lambda (stream)
		   (let ((*connection-params* (connection-parameters conn)))
		     (single-message-case stream)))) stream)))

(defun async-next-message (conn)
  (bb:with-promise (resolve reject)
    (setf (slot-value conn 'callback) (lambda (stream) (resolve stream)))))

(defun async-do-while (promise-fn test-fn)
  (bb:with-promise (resolve reject)
    (labels ((iter ()
	       (bb:aif (bb:attach (funcall promise-fn) test-fn)
		       (resolve)
		       (iter))))
      (iter))))

(defmacro ado-messages ((conn input
			 &key
			   (output (gensym "output"))
			   (finish 'finish))
			&body body)
  (let ((done-name (gensym "done")))
    `(macrolet ((,finish ()
		  `(setf ,',done-name t)))
       (let ((,output (connection-socket conn)))
	 (declare (ignorable ,output))
	 (async-do-while
	  (lambda () (async-next-message ,conn))
	  (lambda (,input)
	    (let ((,done-name nil)
		  (*connection-params* (connection-parameters ,conn)))
	      (single-message-case ,input
		,@body)
	      ,done-name)))))))

(defun async-socket-connect (conn &rest args)
  (with-accessors ((host connection-host)
		   (port connection-port)) conn
    (case host
      (:unix
       (apply #'as:pipe-connect
	      (unix-socket-path *unix-socket-dir* port)
	      args))
      (otherwise
       (apply #'as:tcp-connect host port args)))))

(defun async-initiate-connection (conn)
  (let ((buffer (slot-value conn 'buffer)))
    (setf (connection-parameters conn)
	  (make-hash-table :test 'equal))
    (setf (message-buffer-fill buffer) 0)
    (bb:with-promise (resolve reject)
      (bb:catcher
       (bb:alet*
	   ((nil
	     (bb:with-promise (resolve reject)
	       (async-socket-connect
		conn
		(lambda (sock chunk)
		  (declare (ignorable sock))
		  (push-chunk buffer chunk)
		  (map-messages buffer
				(lambda (stream)
				  (async-handle-message conn stream))))
		(lambda (ev)
		  (with-slots (errback) conn
		    (if errback
			(funcall errback ev)
			(reject ev))))
		:connect-cb
		(lambda (socket)
		  (setf (connection-socket conn)
			(make-instance 'as:async-output-stream :socket socket))
		  (resolve socket)))))
	    (nil
	     (let ((password (connection-password conn)))
	       (startup-message (connection-socket conn)
				(connection-user conn)
				(connection-db conn))
	       (ado-messages (conn r :output w)
		 (#\R
		  (let ((type (read-uint4 r)))
		    (ecase type
		      (0 (finish))
		      (3 (unless password
			   (error "Server requested plain-password authentication, but no password was given."))
		       (plain-password-message w password)
		       (finish))
		      (5 (unless password
			   (error "Server requested md5-password authentication, but no password was given."))
		       (md5-password-message w password
					     (connection-user conn)
					     (read-bytes r 4)))))))))
	    (nil
	     (ado-messages (conn r)
	       (#\K)
	       (#\Z (finish)))))
	 (setf (connection-timestamp-format conn)
	       (if (string= (gethash "integer_datetimes"
				     (connection-parameters conn)) "on")
		   :integer :float))
	 (resolve conn))
       (t (e) (reject e))))))

(defun async-send-parse (conn name query)
  (let ((socket (connection-socket conn)))
    (parse-message socket name query)
    (flush-message socket)
    (ado-messages (conn r)
      (#\1 (finish)))))

(defun async-send-execute (conn name parameters row-handler)
  "Execute a previously parsed query, and apply the given row-reader
to the result."
  (declare (type string name)
           (type list parameters)
           #.*optimize*)
  (let ((socket (connection-socket conn))
	(row-description nil)
	(n-parameters 0)
	(row-handler (etypecase row-handler
		       (function row-handler)
		       (symbol (symbol-function row-handler)))))
    (declare (type (unsigned-byte 16) n-parameters)
	     (type function row-handler))
    (bb:alet*
	((nil
	  (progn
	    (describe-prepared-message socket name)
	    (flush-message socket)
	    (ado-messages (conn r)
	      ;; ParameterDescription
	      (#\t (setf n-parameters (read-uint2 r))
		   (finish)))))
	 (nil
	  (ado-messages (conn r)
	    (#\T (setf row-description (read-field-descriptions r))
		 (finish))
	    (#\n (finish))))
	 (nil
	  (progn
	    (unless (= (length parameters) n-parameters)
	      (error 'database-error
		     :message (format nil "Incorrect number of parameters given for prepared statement ~A." name)))
	    (bind-message socket name (map 'vector 'field-binary-p row-description)
			  parameters)
	    (simple-execute-message socket)
	    (sync-message socket)
	    (ado-messages (conn r)
	      ;; BindComplete
	      (#\2 (finish))))))
      (ado-messages (conn r)
	(#\C)
	(#\D (let ((*timestamp-format* (connection-timestamp-format conn)))
	       (funcall row-handler row-description r)))
	(#\Z (finish))))))

(defun read-field (stream field)
  (declare (type field-description field))
  (let ((size (read-int4 stream)))
    (declare (type (signed-byte 32) size))
    (if (= size -1)
	:null
	(funcall (field-interpreter field)
		 stream size))))

;;; FIXME code reuse gone wild, down with clarity
(defun row-handler-by-reader (callback reader)
  (lambda (fields stream)
    (let ((stream
	    (make-concatenated-stream
	     (flexi-streams:make-in-memory-input-stream
	      #(68 0 0 0 4))
	     stream
	     (flexi-streams:make-in-memory-input-stream
	      #(67 0 0 0 4 0 0)))))
      (funcall callback
	       (funcall reader stream fields)))))

(defun async-open-database (database user password host
			    &optional (port 5432) (use-ssl :no) (service "postgres"))
  (check-type database string)
  (check-type user string)
  (check-type password (or null string))
  (check-type host (or string (eql :unix)) "a string or :unix")
  (check-type port (integer 1 65535) "an integer from 1 to 65535")
  (check-type use-ssl (member :no :yes :try) ":no, :yes, or :try")
  (async-initiate-connection
   (make-instance 'async-database-connection
		  :host host :port port :user user
		  :password password :socket nil :db database :ssl use-ssl
		  :service service)))
