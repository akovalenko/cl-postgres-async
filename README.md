cl-postgres-async
=================

Asynchronous CL-POSTGRES (prototype)

What's working:

* TCP connect with password-based auth, plain or MD5.
* UNIX socket connect (when CL-ASYNC supports PIPE-CONNECT)
* async-send-parse, async-send-execute (the latter returns affected
  row count)
* result set row handling

Minimal demo: https://gist.github.com/akovalenko/0721e6c5d0b660b893f8
