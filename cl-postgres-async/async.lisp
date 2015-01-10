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
   (message-promise :initform nil)))

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
  (with-slots (message-promise) conn
    (let ((was-waiting (shiftf message-promise nil)))
      (if was-waiting
	  (blackbird-base:finish was-waiting stream)
	  (let ((*connection-params*
		  (connection-parameters conn)))
	    (single-message-case stream))))))

(defun async-next-message (conn)
  (with-slots (message-promise) conn
    (assert (not message-promise) () "Nested ASYNC-NEXT-MESSAGE on ~A" conn)
    (setf message-promise (blackbird-base:make-promise :name "ASYNC-NEXT-MESSAGE"))))

(defun async-do-while (promise-fn test-fn)
  (bb:with-promise (resolve reject)
    (labels ((iter ()
	       (bb:catcher
		(bb:alet* ((value (funcall promise-fn)))
		  (bb:aif (funcall test-fn value)
			  (resolve)
			  (iter)))
		(condition (c) (reject c)))))
      (iter))))

(defmacro aprogn (&body body)
  (let (last)
    `(bb:alet* ,(loop for (head . tail) on body
		      when tail
			collect `(nil ,head)
		      else do (setf last head))
       ,last)))

(defmacro ado-messages ((conn input
			 &key
			   (output (gensym "output"))
			   (finish 'finish))
			&body body)
  (let ((done-name (gensym "done")))
    `(macrolet ((,finish ()
		  `(setf ,',done-name t)))
       (let ((,output (connection-socket ,conn)))
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
    (aprogn
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
	   (with-slots (message-promise) conn
	     (if message-promise
		 (bb:signal-error message-promise ev)
		 (reject ev))))
	 :connect-cb
	 (lambda (socket)
	   (setf (connection-socket conn)
		 (make-instance 'as:async-output-stream :socket socket))
	   (resolve socket))))
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
				      (read-bytes r 4))))))))
      (ado-messages (conn r)
	(#\K)
	(#\Z (finish)))
      (progn
	(setf (connection-timestamp-format conn)
	      (if (string= (gethash "integer_datetimes"
				    (connection-parameters conn)) "on")
		  :integer :float))
	conn))))

(defun try-to-sync-async (conn sync-sent)
  (unless sync-sent
    (sync-message (connection-socket conn)))
  (ado-messages (conn r)
    (#\Z (finish))
    (t :skip)))

(defmacro aprog1 (form &body body)
  (let ((values-name (gensym "values-")))
    `(bb:tap ,form (lambda (&rest ,values-name)
		     (declare (ignore ,values-name))
		     ,@body))))

(defmacro with-async-syncing ((conn) &body body)
  (let ((ok (gensym "ok-"))
	(sync-sent (gensym "sync-sent-")))
    `(let ((,sync-sent nil)
	   (,ok nil))
       (bb:finally
	   (aprog1
	       (flet ((sync-message (socket)
			(setf ,sync-sent t)
			(sync-message socket)))
		 (declare (ignorable #'sync-message))
		 ,@body)
	     (setf ,ok t))
	 (unless ,ok
	   (try-to-sync-async ,conn ,sync-sent))))))

(defmacro with-async-query ((query) &body body)
  (let ((time-name (gensym))
	(query-name (gensym)))
    `(let ((,query-name ,query)
           (,time-name (if *query-callback* (get-internal-real-time) 0)))
       (aprog1 (progn ,@body)
	 (when *query-callback*
	   (funcall *query-callback*
		    ,query-name
		    (- (get-internal-real-time) ,time-name)))))))

(defmacro using-async-connection ((connection) &body body)
  (let ((connection-name (gensym)))
    `(let* ((,connection-name ,connection))
       (when (not (connection-available ,connection-name))
	 (error 'database-error
		:message "This connection is still processing another query."))
       (setf (connection-available ,connection-name) nil)
       (bb:finally (progn ,@body)
	 (setf (connection-available ,connection-name) t)))))

(defun async-send-parse (conn name query)
  (with-async-syncing (conn)
    (with-async-query (query)
     (let ((socket (connection-socket conn)))
       (parse-message socket name query)
       (flush-message socket)
       (ado-messages (conn r)
	 (#\1 (finish)))))))

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
		       (symbol (symbol-function row-handler))))
	(affected-rows nil))
    (declare (type (unsigned-byte 16) n-parameters)
	     (type function row-handler))
    (with-async-syncing (conn)
      (aprogn
	(progn
	  (describe-prepared-message socket name)
	  (flush-message socket)
	  (ado-messages (conn r)
	    ;; ParameterDescription
	    (#\t (setf n-parameters (read-uint2 r))
		 (finish))))
	(ado-messages (conn r)
	  (#\T (setf row-description (read-field-descriptions r))
	       (finish))
	  (#\n (finish)))
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
	    (#\2 (finish))))
	(ado-messages (conn r)
	  (#\C (let* ((command-tag (read-str r))
		      (space (position #\Space command-tag :from-end t)))
		 (when space
		   (setf affected-rows
			 (parse-integer command-tag :junk-allowed t
						    :start (1+ space))))))
	  (#\D (let ((*timestamp-format* (connection-timestamp-format conn)))
		 (funcall row-handler row-description r)))
	  (#\Z (finish)))
	affected-rows))))

(defun async-send-query (conn query row-handler)
  (declare (type string query) #.*optimize*)
  (let ((socket (connection-socket conn))
	(row-description nil)
	(affected-rows nil)
	(row-handler (etypecase row-handler
		       (function row-handler)
		       (symbol (symbol-function row-handler)))))
    (with-async-syncing (conn)
      (with-async-query (query)
	(aprogn
	  (progn
	    (simple-parse-message socket query)
	    (simple-describe-message socket)
	    (flush-message socket)
	    (ado-messages (conn r)
	      (#\1 (finish))))
	  (ado-messages (conn r)
	    (#\t :skip)
	    (#\T (setf row-description (read-field-descriptions r))
		 (finish))
	    (#\n (finish)))
	  (progn
	    (simple-bind-message socket (map 'vector 'field-binary-p row-description))
	    (simple-execute-message socket)
	    (sync-message socket)
	    (ado-messages (conn r)
	      (#\2 (finish))))
	  (ado-messages (conn r)
	    (#\C (let* ((command-tag (read-str r))
			(space (position #\Space command-tag :from-end t)))
		   (when space
		     (setf affected-rows
			   (parse-integer command-tag :junk-allowed t
						      :start (1+ space))))))
	    (#\D (let ((*timestamp-format* (connection-timestamp-format conn)))
		   (funcall row-handler row-description r)))
	    (#\Z (finish)))
	  affected-rows)))))

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

(defun async-reopen-database (conn)
  (unless (database-open-p conn)
    (async-initiate-connection conn)))

(defun ignore-row-handler (fields stream)
  (declare (ignore fields stream)))

(defun async-exec-query (connection query
			 &optional (row-handler #'ignore-row-handler))
  (check-type query string)
  (using-async-connection (connection)
    (async-send-query connection query row-handler)))

(defun async-prepare-query (connection name query)
  (check-type query string)
  (using-async-connection (connection)
    (async-send-parse connection name query)))

(defun async-exec-prepared (connection name parameters
			    &optional (row-handler #'ignore-row-handler))
  (check-type name string)
  (check-type parameters list)
  (using-async-connection (connection)
    (async-send-execute connection name parameters row-handler)))

