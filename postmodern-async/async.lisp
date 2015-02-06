(in-package :postmodern-async)
(pushnew '*database* bb:*promise-keep-specials*)

(defclass pooled-async-database-connection (async-database-connection)
  ((pool-type :initarg :pool-type :accessor connection-pool-type))
  (:documentation "Type for database connections that are pooled.
Stores the arguments used to create it, so different pools can be
distinguished."))

(defmethod disconnect ((connection pooled-async-database-connection))
  "Add the connection to the corresponding pool, or drop it when the
pool is full."
  (macrolet ((the-pool ()
               '(gethash (connection-pool-type connection) *connection-pools* ())))
    (when (database-open-p connection)
      (with-pool-lock
        (if (or (not *max-pool-size*) (< (length (the-pool)) *max-pool-size*))
            (push connection (the-pool))
            (call-next-method))))
    (values)))

(defun async-connect (database user password host
		      &key (port 5432) pooled-p
			(use-ssl *default-use-ssl*) (service "postgres"))
  "Create and return a database connection."
  (cond (pooled-p
         (let ((type (list database user password host port use-ssl)))
           (or (get-from-pool type)
               (bb:alet* ((connection
			   (async-open-database
			    database user password host port use-ssl)))
                 (change-class connection
			       'pooled-async-database-connection :pool-type type)))))
        (t (async-open-database database user password host port use-ssl service))))

(defmacro async-query (query &rest args/format)
  "Execute a query, optionally with arguments to put in the place of
$X elements. If one of the arguments is a known result style or a class name,
it specifies the format in which the results should be returned."
  (let* ((format :rows)
         (args (loop :for arg :in args/format
		     :if (or (dao-spec-for-format arg)
			     (assoc arg *result-styles*)) :do (setf format arg)
                     :else :collect arg)))
    (destructuring-bind (reader result-form) (reader-for-format format)
      (let*
	  ((handler `(cl-postgres-async::row-handler-by-reader #'add-row ,reader))
	   (base (if args
		     `(bb:walk
			(async-prepare-query  *database* "" ,(real-query query))
			(async-exec-prepared *database* "" (list ,@args) ,handler))
		     `(async-exec-query *database* ,(real-query query) ,handler))))
	`(let (result tail)
	   (flet ((add-row (list)
		    (if result
			(setf (cdr tail) list
			      tail list)
			(setf result list
			      tail list))))
	     (bb:alet* ((affected ,base))
	       (,result-form (values result affected)))))))))

(defmacro async-execute (query &rest args)
  "Execute a query, ignore the results."
  `(bb:multiple-promise-bind (nil rows)
			     (async-query ,query ,@args :none)
     (if rows (values rows rows) 0)))

(defmacro with-async-connection (spec &body body)
  (let ((conn (gensym)))
    `(bb:alet* ((,conn (apply #'async-connect ,spec)))
       (let ((*database* ,conn))
	 (bb:finally (bb:walk ,@body)
	   (disconnect ,conn))))))

(defmacro do-async-query (query (&rest names) &body body)
  (let* ((fields (gensym))
	 (query-name (gensym))
	 args
	 (reader-expr
	   `(row-reader (,fields)
	      (unless (= ,(length names) (length ,fields))
		(error "Number of field names does not match number of selected fields in query ~A." ,query-name))
	      (progn (next-row)
		     (let ,(loop :for i :from 0
				 :for name :in names
				 :collect `(,name (next-field (elt ,fields ,i))))
		       ,@body)))))
    (when (and (consp query) (not (keywordp (first query))))
      (setf args (cdr query) query (car query)))
    (if args
	`(let ((,query-name ,(real-query query)))
	   (bb:walk
	     (async-prepare-query *database* "" ,query-name)
	     (async-exec-prepared *database* "" (list ,@args)
				  (cl-postgres-async::row-handler-by-reader
				   #'identity ,reader-expr))))
	`(let ((,query-name ,(real-query query)))
	   (async-exec-query *database* ,query-name
			     (cl-postgres-async::row-handler-by-reader
			      #'identity ,reader-expr))))))

(defun async-ensure-prepared (connection id query)
  (let ((meta (connection-meta connection)))
    (unless (gethash id meta)
      (setf (gethash id meta) t)
      (async-prepare-query connection (symbol-name id) query))))

(defun async-generate-prepared (function-form query format)
  "Helper macro for the following two functions."
  (destructuring-bind (reader result-form) (reader-for-format format)
    (let* ((base `(async-exec-prepared *database* (symbol-name statement-id) params
				       (cl-postgres-async::row-handler-by-reader
					#'add-row ,reader)))
	   (wrapped
	     `(let (result tail)
		(flet ((add-row (list)
			 (if result
			     (setf (cdr tail) list
				   tail list)
			     (setf result list
				   tail list))))
		  (bb:alet* ((affected ,base))
		    (values result affected))))))
      `(let ((statement-id (next-statement-id))
             (query ,(real-query query)))
	 (,@function-form (&rest params)
			  (bb:walk
			    (async-ensure-prepared *database* statement-id query)
			    (,result-form ,wrapped)))))))

(defmacro async-prepare (query &optional (format :rows))
  (async-generate-prepared `(lambda) query format))

(defmacro def-async-prepared (name query &optional (format :rows))
  "Like async-prepare, but gives the function a name instead of returning
it."
  (async-generate-prepared `(defun ,name) query format))

(defmacro def-async-prepared-with-names (name (&rest args)
					 (query &rest query-args)
					 &optional (format :rows))
  "Like def-async-prepared, but with lambda list for statement arguments."
  (let ((prepared-name (gensym "STATEMENT")))
    `(progn
       (def-async-prepared ,prepared-name ,query ,format)
       (defun ,name ,args
	 (,prepared-name ,@query-args)))))

(defun async-sequence-next (sequence)
  "Shortcut for getting the next value from a sequence."
  (async-query (:select (:nextval (to-identifier sequence))) :single))

(defun async-list-tables (&optional strings-p)
  "Return a list of the tables in a database. Turn them into keywords
if strings-p is not true."
  (bb:alet ((result (async-query (make-list-query "r") :column)))
    (if strings-p result (mapcar 'from-sql-name result))))

(defun async-table-exists-p (table)
  "Check whether a table exists. Takes either a string or a symbol for
the table name."
  (async-query (make-exists-query "r" table) :single))

(defun async-list-sequences (&optional strings-p)
  "Return a list of the sequences in a database. Turn them into
keywords if strings-p is not true."
  (bb:alet ((result (async-query (make-list-query "S") :column)))
    (if strings-p result (mapcar 'from-sql-name result))))

(defun async-sequence-exists-p (sequence)
  "Check whether a sequence exists. Takes either a string or a symbol
for the sequence name."
  (async-query (make-exists-query "S" sequence) :single))

(defun async-list-views (&optional strings-p)
  "Return a list of the views in a database. Turn them into keywords
if strings-p is not true."
  (bb:alet ((result (async-query (make-list-query "v") :column)))
    (if strings-p result (mapcar 'from-sql-name result))))

(defun async-view-exists-p (view)
  "Check whether a view exists. Takes either a string or a symbol for
the view name."
  (async-query (make-exists-query "v" view) :single))

(defun async-table-description (table &optional schema-name)
  "Return a list of (name type null-allowed) lists for the fields of a
table.  If SCHEMA-NAME is specified, only fields from that schema are
returned."
  (let ((schema-test (if schema-name (sql (:= 'pg-namespace.nspname schema-name)) "true")))
    (bb:alet ((rows
	       (async-query (:order-by (:select 'attname 'typname (:not 'attnotnull) 'attnum :distinct
                               :from 'pg-catalog.pg-attribute
                               :inner-join 'pg-catalog.pg-type :on (:= 'pg-type.oid 'atttypid)
                               :inner-join 'pg-catalog.pg-class :on (:and (:= 'pg-class.oid 'attrelid)
                                                                          (:= 'pg-class.relname (to-identifier table)))
                               :inner-join 'pg-catalog.pg-namespace :on (:= 'pg-namespace.oid 'pg-class.relnamespace)
                               :where (:and (:> 'attnum 0) (:raw schema-test)))
				       'attnum))))
      (mapcar #'butlast rows))))


(defun async-commit-transaction (transaction)
  "Immediately commit an open transaction."
  (when (transaction-open-p transaction)
    (bb:walk
      (let ((*database* (transaction-connection transaction)))
	(async-execute "COMMIT"))
      (progn
	(setf (transaction-open-p transaction) nil)
	(mapc #'funcall (commit-hooks transaction))))))

(defun async-abort-transaction (transaction)
  (when (transaction-open-p transaction)
    (bb:walk
      (let ((*database* (transaction-connection transaction)))
	(async-execute "ABORT"))
      (progn
	(setf (transaction-open-p transaction) nil)
	(mapc #'funcall (abort-hooks transaction))))))

(defun call-with-async-transaction (body)
  (let ((transaction (make-instance 'transaction-handle)))
    (incf (cl-postgres-async::adb-transaction-level *database*))
    (push transaction (cl-postgres-async::adb-transaction-stack *database*))
    (bb:finally
	(bb:walk
	  (async-execute "BEGIN")
	  (funcall body transaction)
	  (async-commit-transaction transaction))
      (decf (cl-postgres-async::adb-transaction-level *database*))
      (pop (cl-postgres-async::adb-transaction-stack *database*))
      (async-abort-transaction transaction))))

(defmacro with-async-transaction ((&optional name) &body body)
  "Execute the body within a database transaction, committing when the
body exits normally, and aborting otherwise. An optional name can be
given to the transaction, which can be used to force a commit or abort
before the body unwinds."
  (if name
      `(call-with-async-transaction (lambda (,name) (bb:walk ,@body)))
      (let ((ignored (gensym)))
        `(call-with-async-transaction
	  (lambda (,ignored) (declare (ignore ,ignored)) (bb:walk ,@body))))))


(defun async-release-savepoint (savepoint)
  "Immediately release a savepoint, commiting its results."
  (when (savepoint-open-p savepoint)
    (let ((*database* (savepoint-connection savepoint)))
      (bb:walk
	(async-execute (format nil "RELEASE SAVEPOINT ~A"
			       (savepoint-name savepoint)))
	(progn
	  (setf (transaction-open-p savepoint) nil)
	  (mapc #'funcall (commit-hooks savepoint)))))))

(defun async-rollback-savepoint (savepoint)
  "Immediately roll back a savepoint, aborting it results."
  (when (savepoint-open-p savepoint)
    (let ((*database* (savepoint-connection savepoint)))
      (bb:walk
	(async-execute (format nil "ROLLBACK TO SAVEPOINT ~A"
			       (savepoint-name savepoint)))
	(progn
	  (setf (savepoint-open-p savepoint) nil)
	  (mapc #'funcall (abort-hooks savepoint)))))))

(defun call-with-async-savepoint (name body)
  (let ((savepoint (make-instance 'savepoint-handle :name (to-sql-name name))))
    (incf (cl-postgres-async::adb-transaction-level *database*))
    (push savepoint (cl-postgres-async::adb-transaction-stack *database*))
    (bb:finally
	(bb:walk
	  (async-execute (format nil "SAVEPOINT ~A" (savepoint-name savepoint)))
	  (funcall body savepoint)
	  (async-release-savepoint savepoint))
      (progn
      	(decf (cl-postgres-async::adb-transaction-level *database*))
	(pop (cl-postgres-async::adb-transaction-stack *database*))
	(async-rollback-savepoint savepoint)))))

(defun call-with-async-logical-transaction (name body)
  (if (zerop (cl-postgres-async::adb-transaction-level *database*))
      (call-with-async-transaction body)
      (call-with-async-savepoint name body)))

(defmacro with-async-logical-transaction ((&optional (name nil name-p)) &body body)
  (let* ((effective-name (if name-p
                             name
                             (gensym)))
	 (effective-body (if name-p
                             `(lambda (,name) (bb:walk ,@body))
                             `(lambda (,effective-name)
                                (declare (ignore ,effective-name))
                                (bb:walk ,@body)))))
    `(call-with-async-logical-transaction ',effective-name ,effective-body)))

(defun call-with-ensured-async-transaction (thunk)
  (if (zerop (cl-postgres-async::adb-transaction-level *database*))
      (with-async-transaction () (funcall thunk))
      (funcall thunk)))

(defmacro ensure-async-transaction (&body body)
  "Executes body within a with-transaction form if and only if no
transaction is already in progress."
  `(call-with-ensured-async-transaction (lambda () (bb:walk ,@body))))
