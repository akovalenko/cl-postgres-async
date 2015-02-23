(defpackage :simple-date-system
  (:use :common-lisp :asdf))
(in-package :simple-date-system)

(defsystem :simple-date
  :components 
  ((:module :simple-date
            :components ((:file "simple-date")))))

(defsystem :simple-date-postgres-async-glue
  :depends-on (:simple-date :cl-postgres-async)
  :components
  ((:module :simple-date
            :components
            ((:file "cl-postgres-glue")))))

(defsystem :simple-date-tests
  :depends-on (:eos :simple-date)
  :components
  ((:module :simple-date
            :components ((:file "tests")))))

(defmethod perform ((op asdf:test-op) (system (eql (find-system :simple-date))))
  (asdf:oos 'asdf:load-op :simple-date-tests)
  (funcall (intern (string :run!) (string :Eos)) :simple-date))

(defmethod perform :after ((op asdf:load-op) (system (eql (find-system :simple-date))))
  (when (and (find-package :cl-postgres-async)
             (not (find-symbol (symbol-name '#:+apomo-postgres-day-offset+) :simple-date)))
    (asdf:oos 'asdf:load-op :simple-date-postgres-async-glue)))
