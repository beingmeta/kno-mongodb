;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

;;; This is a simple ORM for operating on objects stored in mongodb,
;;; including OIDs stored in mongodb pools. The MGO/{ADD|DROP|STORE}
;;; calls connect directly to MongoDB and then update the local object
;;; as needed as well.

(in-module 'mongodb/xcursor)

(use-module '{varconfig logger fifo ezrecords})
(use-module '{mongodb})

(module-export! '{mongodb/xcursor
		  xcursor/open xcursor/close!
		  xcursor/readvec xcursor/read
		  xcursor/done? xcursor/readcount
		  xcursor/skipcount xcursor/skip})

(define %loglevel %notice%)

(defrecord (req opaque mutable)
  count
  (condvar (make-condvar))
  (result {})
  (done #f))

(define (xcursor->string xcursor)
  (local collection (xcursor-collection xcursor))
  (stringout "#<xcursor " (mkpath (mongodb/dburi collection)
				  (collection/name collection))
    " " (xcursor-query xcursor) " " (hashref xcursor) ">"))

(defrecord (xcursor mutable opaque `(stringfn . xcursor->string))
  collection
  query
  opts
  (fifo (make-fifo))
  (condvar (make-condvar))
  (skipped 0)
  (cursor #f)
  (thread #f))
(module-export! '{xcursor-collection xcursor-query xcursor-opts xcursor-thread})

(define (reqloop xcursor)
  (let ((fifo (xcursor-fifo xcursor))
	(condvar (xcursor-condvar xcursor))
	(cursor (xcursor-cursor xcursor))
	(collection (xcursor-collection xcursor))
	(query (xcursor-query xcursor))
	(opts (xcursor-opts xcursor)))
    (unless cursor
      (with-lock condvar
	(set! cursor (cursor/open collection query opts))
	(set-xcursor-cursor! xcursor cursor)))
    (while (fifo-live? fifo)
      (let ((req (fifo/pop fifo)))
	(when (req? req)
	  (let* ((count (req-count req))
		 (req-condvar (req-condvar req))
		 (result (onerror 
			     (cursor/readvec cursor count)
			     (lambda (ex) ex))))
	    (unwind-protect
		(begin (condvar/lock! req-condvar)
		  (when (and (fail? result) (cursor/done? cursor))
		    (fifo/readonly! fifo #t))
		  (set-req-result! req result)
		  (set-req-done! req #t)
		  (condvar/signal req-condvar #t))
	      (condvar/unlock! req-condvar))))))))

(define (xcursor-start xcursor)
  (with-lock (xcursor-condvar xcursor)
    (unless (xcursor-thread xcursor)
      (set-xcursor-thread! xcursor (thread/call reqloop xcursor))
      (lognotice |XCursorStart|
	"Spawned thread " (thread-id (xcursor-thread xcursor)) 
	" (<" (hashref (xcursor-thread xcursor)) ">) for xcursor \n  "
	xcursor))))

(define (xcursor/open collection query (opts #f))
  (let* ((name (glom (collection/name collection) "-fifo"))
	 (fifo (fifo/make (glom (collection/name collection) "-feed") [name name async #t]))
	 (xcursor (cons-xcursor collection query opts fifo)))
    xcursor))
(define mongodb/xcursor (fcn/alias xcursor/open))

(define (xcursor/readvec arg (n 1) (fifo))
  (if (xcursor? arg)
      (set! fifo (xcursor-fifo arg))
      (irritant arg |NotAMongoXCursor|))
  (unless (xcursor-thread arg) (xcursor-start arg))
  (tryif (fifo-live? fifo)
    (let* ((request (cons-req n))
	   (condvar (req-condvar request)))
      (condvar/lock! condvar)
      (fifo/push! fifo request)
      (while (and (fifo-live? fifo) (not (req-done request)))
	(condvar/wait condvar))
      (condvar/unlock! condvar)
      (if (req-done request)
	  (req-result request)
	  (fail)))))

(define (xcursor/read arg (n 1))
  (let ((vec (xcursor/readvec arg n)))
    (if (fail? vec) vec
	(if (vector? vec)
	    (elts vec)
	    vec))))

(define (xcursor/close! arg)
  (unless (xcursor? arg) (irritant arg |NotAMongoXCursor|))
  (fifo/close! (xcursor-fifo arg))
  (when (xcursor-cursor arg) (cursor/close! (xcursor-cursor arg))))

(define (xcursor/done? arg)
  (unless (xcursor? arg) (irritant arg |NotAMongoXCursor|))
  (and (xcursor-cursor arg) (cursor/done? (xcursor-cursor arg))))

(define (xcursor/readcount arg)
  (unless (xcursor? arg) (irritant arg |NotAMongoXCursor|))
  (if (xcursor-cursor arg)
      (- (cursor/readcount (xcursor-cursor arg))
	 (fifo/load (xcursor-fifo arg)))
      0))

(define (xcursor/skipcount arg)
  (unless (xcursor? arg) (irritant arg |NotAMongoXCursor|))
  (xcursor-skipcount arg))

(define (xcursor/skip arg)
  (unless (xcursor? arg) (irritant arg |NotAMongoXCursor|))
  (error |NotYetImplemented| xcursor/skip))


