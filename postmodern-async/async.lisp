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
      (let ((base (if args
                      `(bb:alet*
			   ((nil (async-prepare-query  *database* "" ,(real-query query)))
			    (result
			     (async-exec-prepared *database* "" (list ,@args) ,reader)))
			 result)
                      `(async-exec-query *database* ,(real-query query) ,reader))))
        `(,result-form ,base)))))
