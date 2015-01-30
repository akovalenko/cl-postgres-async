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
