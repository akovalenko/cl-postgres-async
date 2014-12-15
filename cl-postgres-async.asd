(defpackage :cl-postgres-async-system
  (:use :common-lisp :asdf))
(in-package :cl-postgres-async-system)

;; Change this to enable/disable unicode manually (mind that it won't
;; work unless your implementation supports it).
(defparameter *unicode*
  #+(or sb-unicode unicode ics openmcl-unicode-strings) t
  #-(or sb-unicode unicode ics openmcl-unicode-strings) nil)
(defparameter *string-file* (if *unicode* "strings-utf-8" "strings-ascii"))

(defsystem :cl-postgres-async
  :description "Low-level async client library for PosgreSQL"
  :depends-on (:md5
	       :cl-async
	       :blackbird
	       :fast-io
	       :flexi-streams
               #-(or sbcl allegro ccl) :usocket
               #+sbcl                  :sb-bsd-sockets)
  :components 
  ((:module :cl-postgres-async
            :components ((:file "trivial-utf-8")
                         (:file "ieee-floats")
                         (:file "package")
                         (:file "errors" :depends-on ("package"))
                         (:file "sql-string" :depends-on ("package"))
                         (:file #.*string-file* :depends-on ("package" "trivial-utf-8"))
                         (:file "communicate" :depends-on (#.*string-file* "sql-string"))
                         (:file "messages" :depends-on ("communicate"))
                         (:file "interpret" :depends-on ("communicate" "ieee-floats"))
                         (:file "protocol" :depends-on ("interpret" "messages" "errors"))
                         (:file "public" :depends-on ("protocol"))
                         (:file "bulk-copy" :depends-on ("public"))
			 (:file "async" :depends-on ("public"))))))

(defsystem :cl-postgres-async-tests
  :depends-on (:cl-postgres-async :eos :simple-date)
  :components
  ((:module :cl-postgres-async
            :components ((:file "tests")))))

(defmethod perform ((op asdf:test-op) (system (eql (find-system :cl-postgres-async))))
  (asdf:oos 'asdf:load-op :cl-postgres-async-tests)
  (funcall (intern (string :prompt-connection) (string :cl-postgres-async-tests)))
  (funcall (intern (string :run!) (string :Eos)) :cl-postgres-async))

(defmethod perform :after ((op asdf:load-op) (system (eql (find-system :cl-postgres-async))))
  (when (and (find-package :simple-date)
             (not (find-symbol (symbol-name '#:+postgres-day-offset+) :simple-date)))
    (asdf:oos 'asdf:load-op :simple-date-postgres-glue)))
