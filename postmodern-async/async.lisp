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

(defmacro with-async-savepoint (name &body body)
  `(call-with-async-savepoint
    ',name
    (lambda (,name) (declare (ignorable ,name))
      (bb:walk ,@body))))

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

(defun async-query-dao% (type query row-reader &rest args)
  (let ((class (find-class type)))
    (unless (class-finalized-p class)
      (finalize-inheritance class))
    (let (result tail)
      (flet ((add-row (list)
	       (if result
		   (setf (cdr tail) list
			 tail list)
		   (setf result list
			 tail list))))
	(let ((row-handler
		(cl-postgres-async::row-handler-by-reader
		 #'add-row row-reader)))
	  (bb:walk
	    (if args
		(bb:walk
		  (async-prepare-query *database* "" query)
		  (async-exec-prepared *database* "" args row-handler))
		(async-exec-query *database* query row-handler))
	    result))))))

(defmacro async-query-dao (type query &rest args)
  `(flet ((query-dao% (&rest args)
	    (apply #'async-query-dao% args)))
     (query-dao ,type ,query ,@args)))

(defmacro async-select-dao (type &optional (test t) &rest ordering)
  "Select daos for the rows in its table for which the given test
holds, order them by the given criteria."
  `(async-query-dao% ,type (sql ,(generate-dao-query type test ordering))
		     (dao-row-reader (find-class ,type))))




(defgeneric async-dao-exists-p (dao)
  (:documentation "Return a boolean indicating whether the given dao
  exists in the database."))
(defgeneric async-insert-dao (dao)
  (:documentation "Insert the given object into the database."))
(defgeneric async-update-dao (dao)
  (:documentation "Update the object's representation in the database
  with the values in the given instance."))
(defgeneric async-delete-dao (dao)
  (:documentation "Delete the given dao from the database."))
(defgeneric async-upsert-dao (dao)
  (:documentation "Update or insert the given dao.  If its primary key
  is already in the database and all slots are bound, an update will
  occur.  Otherwise it tries to insert it."))
(defgeneric async-get-dao (type &rest args)
  (:method ((class-name symbol) &rest args)
    (let ((class (find-class class-name)))
      (if (class-finalized-p class)
          (error "Class ~a has no key slots." (class-name class))
          (finalize-inheritance class))
      (apply 'async-get-dao class-name args)))
  (:documentation "Get the object corresponding to the given primary
  key, or return nil if it does not exist."))
(defgeneric async-make-dao (type &rest args &key &allow-other-keys)
  (:method ((class-name symbol) &rest args &key &allow-other-keys)
    (let ((class (find-class class-name)))
      (apply 'async-make-dao class args)))
  (:method ((class dao-class) &rest args &key &allow-other-keys)
    (unless (class-finalized-p class)
      (finalize-inheritance class))
    (let ((instance (apply #'make-instance class args)))
      (async-insert-dao instance)))
  (:documentation "Make the instance of the given class and insert it into the database"))

(defun build-async-dao-methods (class)
  "Synthesise a number of methods for a newly defined DAO class.
\(Done this way because some of them are not defined in every
situation, and each of them needs to close over some pre-computed
values.)"

  #- (and)
  (setf (slot-value class 'column-map)
	(mapcar (lambda (s) (cons (slot-sql-name s) (slot-definition-name s))) (dao-column-slots class)))

  (%eval
   `(let* ((fields (dao-column-fields ,class))
	   (key-fields (dao-keys ,class))
	   (ghost-slots (remove-if-not 'ghost (dao-column-slots ,class)))
	   (ghost-fields (mapcar 'slot-definition-name ghost-slots))
	   (value-fields (remove-if (lambda (x) (or (member x key-fields) (member x ghost-fields))) fields))
	   (table-name (dao-table-name ,class)))
      (labels ((field-sql-name (field)
		 (make-symbol (car (find field (slot-value ,class 'column-map) :key #'cdr :test #'eql))))
	       (test-fields (fields)
		 `(:and ,@(loop :for field :in fields :collect (list := (field-sql-name field) '$$))))
	       (set-fields (fields)
		 (loop :for field :in fields :append (list (field-sql-name field) '$$)))
	       (slot-values (object &rest slots)
		 (loop :for slot :in (apply 'append slots) :collect (slot-value object slot))))

	;; When there is no primary key, a lot of methods make no sense.
	(when key-fields
	  (let ((tmpl (sql-template `(:select (:exists (:select t :from ,table-name
							:where ,(test-fields key-fields)))))))
	    (defmethod async-dao-exists-p ((object ,class))
	      (and (every (lambda (s) (slot-boundp object s)) key-fields)
		   (async-query (apply tmpl (slot-values object key-fields)) :single))))

	  ;; When all values are primary keys, updating makes no sense.
	  (when value-fields
	    (let ((tmpl (sql-template `(:update ,table-name :set ,@(set-fields value-fields)
					:where ,(test-fields key-fields)))))
	      (defmethod async-update-dao ((object ,class))
		(bb:alet ((n (async-execute (apply tmpl (slot-values object value-fields key-fields)))))
		  (when (zerop n)
		    (error "Updated row does not exist."))
		  object))

	      (defmethod async-upsert-dao ((object ,class))
		(bb:catcher
		 (bb:alet ((n (async-execute (apply tmpl (slot-values object value-fields key-fields)))))
		   (if (zerop n)
		       (bb:walk
			 (async-insert-dao object)
			 (values object t))
		       (values object nil)))
		 (unbound-slot ()
			       (bb:walk
				 (async-insert-dao object)
				 (values object t)))))))

	  (let ((tmpl (sql-template `(:delete-from ,table-name :where ,(test-fields key-fields)))))
	    (defmethod async-delete-dao ((object ,class))
	      (async-execute (apply tmpl (slot-values object key-fields)))))

	  (let ((tmpl (sql-template `(:select * :from ,table-name :where ,(test-fields key-fields)))))
	    (defmethod async-get-dao ((type (eql (class-name ,class))) &rest keys)
	      (let (result)
		(flet ((add-row (list)
			 (setf result (or result (car list)))))
		  (bb:walk
		    (async-exec-query *database*  (apply tmpl keys)
				      (cl-postgres-async::row-handler-by-reader
				       #'add-row (dao-row-reader ,class)))
		    result))))))

	(defmethod async-insert-dao ((object ,class))
	  (let (bound unbound)
	    (loop :for field :in fields
		  :do (if (slot-boundp object field)
			  (push field bound)
			  (push field unbound)))

	    (let* ((values (mapcan (lambda (x) (list (field-sql-name x) (slot-value object x)))
				   (remove-if (lambda (x) (member x ghost-fields)) bound) )))
	      (bb:alet ((returned (async-query (sql-compile `(:insert-into ,table-name
							      :set ,@values
							      ,@(when unbound (cons :returning unbound))))
					       :row)))
		(when unbound
		  (loop :for value :in returned
			:for field :in unbound
			:do (setf (slot-value object field) value)))
		object))))

	(let* ((defaulted-slots (remove-if-not (lambda (x) (slot-boundp x 'col-default))
					       (dao-column-slots ,class)))
	       (defaulted-names (mapcar 'slot-definition-name defaulted-slots))
	       (default-values (mapcar 'column-default defaulted-slots)))
	  (if defaulted-slots
	      (defmethod async-fetch-defaults ((object ,class))
		(let (names defaults)
		  ;; Gather unbound slots and their default expressions.
		  (loop :for slot-name :in defaulted-names
			:for default :in default-values
			:do (unless (slot-boundp object slot-name)
			      (push slot-name names)
			      (push default defaults)))
		  ;; If there are any unbound, defaulted slots, fetch their content.
		  (when names
		    (bb:alet ((list (async-query (sql-compile (cons :select defaults)) :list)))
		      (loop :for value :in list
			    :for slot-name :in names
			    :do (setf (slot-value object slot-name) value))))))
	      (defmethod async-fetch-defaults ((object ,class))
		nil)))
	(defmethod shared-initialize :after ((object ,class) slot-names
					     &key (fetch-defaults nil) &allow-other-keys)
	  (declare (ignore slot-names))
	  (when (and fetch-defaults
		     (typep *database* 'async-database-connection))
	    (error "Not fetching default slots asynchronously")))))))

(defun async-save-dao (dao)
  "Try to insert the content of a DAO. If this leads to a unique key
violation, update it instead."
  (bb:catcher
   (bb:walk (async-insert-dao dao) t)
   (cl-postgres-async-error:unique-violation
    ()
    (bb:walk (async-update-dao dao) nil))))

(defun async-save-dao/transaction (dao)
  (bb:catcher
   (bb:walk (with-async-savepoint async-save-dao/transaction
	      (async-insert-dao dao)) t)
   (cl-postgres-async-error:unique-violation
    ()
    (bb:walk (async-update-dao dao) nil))))


(defmacro do-async-query-dao (((type type-var) query) &body body)
  "Like async-query-dao, but rather than returning a list of results,
executes BODY once for each result, with TYPE-VAR bound to the DAO
representing that result."
  (let (args)
    (when (and (consp query) (not (keywordp (first query))))
      (setf args (cdr query) query (car query)))
    `(async-query-dao% ,type ,(real-query query)
		       (dao-row-reader-with-body (,type ,type-var)
			 ,@body)
		       ,@args)))

(defmacro do-async-select-dao (((type type-var) &optional (test t) &rest ordering)
			       &body body)
  "Like async-select-dao, but rather than returning a list of results,
executes BODY once for each result, with TYPE-VAR bound to the DAO
representing that result."
  `(async-query-dao% ,type (sql ,(generate-dao-query type test ordering))
		     (dao-row-reader-with-body (,type ,type-var)
		       ,@body)))

