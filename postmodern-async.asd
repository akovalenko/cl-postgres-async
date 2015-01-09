(defpackage :postmodern-async-system
  (:use :common-lisp :asdf)
  (:export :*threads*))
(in-package :postmodern-async-system)

;; Change this to manually turn threading support on or off.
(eval-when (:compile-toplevel :load-toplevel :execute)
  #+(or allegro armedbear cmu corman (and digitool ccl-5.1)
        ecl lispworks openmcl sbcl)
  (pushnew :postmodern-async-thread-safe *features*)

  #+(or allegro clisp ecl lispworks mcl openmcl cmu sbcl)
  (pushnew :postmodern-async-use-mop *features*))

(defsystem :postmodern-async
  :description "PosgreSQL programming API"
  :depends-on (:cl-postgres-async :s-sql #+postmodern-async-use-mop :closer-mop
				  #+postmodern-async-thread-safe :bordeaux-threads)
  :components
  ((:module :postmodern-async
            :components ((:file "package")
                         (:file "connect" :depends-on ("package"))
                         (:file "query" :depends-on ("connect"))
                         (:file "prepare" :depends-on ("query"))
                         (:file "util" :depends-on ("query"))
                         (:file "transaction" :depends-on ("query"))
                         (:file "namespace" :depends-on ("query"))
                         #+postmodern-async-use-mop
                         (:file "table" :depends-on ("util" "transaction"))
                         (:file "deftable" :depends-on
                                ("query" #+postmodern-async-use-mop "table"))
			 (:file "async" :depends-on ("util" "transaction"))))))

(defsystem :postmodern-async-tests
  :depends-on (:postmodern :eos :simple-date :simple-date-postgres-glue)
  :components
  ((:module :postmodern
            :components ((:file "tests")))))

(defmethod perform ((op asdf:test-op) (system (eql (find-system :postmodern))))
  (asdf:oos 'asdf:load-op :postmodern)
  (asdf:oos 'asdf:load-op :cl-postgres-async-tests)
  (asdf:oos 'asdf:load-op :postmodern-async-tests)
  (funcall (intern (string :prompt-connection) (string :cl-postgres-tests))
           (eval (intern (string :*test-connection*) (string :postmodern-async-tests))))
  (funcall (intern (string :run!) (string :Eos)) :postmodern))
