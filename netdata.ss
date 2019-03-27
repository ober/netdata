;; -*- Gerbil -*-
package: netdata
namespace: netdata
(export main)

(declare (not optimize-dead-definitions))
(import
  :ober/datadog/dda
  :gerbil/gambit
  :scheme/base
  :std/crypto/cipher
  :std/crypto/etc
  :std/crypto/libcrypto
  :std/db/dbi
  :std/db/postgresql
  :std/db/postgresql-driver
  :std/debug/heap
  :std/format
  :std/generic
  :std/generic/dispatch
  :std/misc/channel
  :std/misc/ports
  :std/net/address
  :std/net/request
  :std/pregexp
  :std/srfi/13
  :std/srfi/19
  :std/srfi/95
  :std/sugar
  :std/text/base64
  :std/text/json
  :std/text/utf8
  :std/text/yaml
  :std/xml/ssax
  )

(import (rename-in :gerbil/gambit/os (current-time builtin-current-time)))

(def DEBUG (getenv "DEBUG" #f))

(def program-name "nd")
(def config-file "~/.netdata.yaml")

(def (dp msg)
  (when DEBUG
    (displayln msg)))

(def interactives
  (hash
   ("active-metric-for-host?" (hash (description: "Poll local netdata server for allmetrics and submit to db.") (usage: "metric? host metric") (count: 2)))
   ("local-get-active-metrics" (hash (description: "Get Jira issue details") (usage: "jira-issue <issue id>") (count: 1)))
   ("netdata" (hash (description: "Poll local netdata server for allmetrics and submit to datadog.") (usage: "netdata <config.yaml>") (count: 1)))
   ("netdata-hosts" (hash (description: "Poll local netdata server for allmetrics and submit to datadog.") (usage: "netdata <server> <port>") (count: 2)))
   ("netdata-loop" (hash (description: "Poll local netdata server for allmetrics and submit to datadog.") (usage: "netdata-loop <config.yaml>") (count: 1)))
   ("clear-bundle" (hash (description: "Clear postgresql bundle table to datadog") (usage: "clear-bundle config-file") (count: 1)))))

(def (main . args)
  (if (null? args)
    (usage))
  (let* ((argc (length args))
	 (verb (car args))
	 (args2 (cdr args)))
    (unless (hash-key? interactives verb)
      (usage))
    (let* ((info (hash-get interactives verb))
	   (count (hash-get info count:)))
      (unless count
	(set! count 0))
      (unless (= (length args2) count)
	(usage-verb verb))
      (apply (eval (string->symbol (string-append "netdata#" verb))) args2))))

;; netdata
(def (netdata-loop config)
  (let* ((config (car (yaml-load config)))
	 (server (hash-get config "netdata-server"))
	 (postgres-user (hash-get config "postgres-user"))
	 (port (hash-get config "netdata-port"))
	 (hosts (hash-get config "hosts"))
	 (loop-seconds (hash-get config "loop-seconds"))
	 (dd-max-retries (or (hash-get config "dd-max-retries") 3))
	 (submit-metrics (hash-get config "metrics")))
    (while #t
      (for-each
	(lambda (host)
	  (netdata-get-metrics server port host submit-metrics #f (hash) (hash)))
	hosts)
      (displayln "sleeping... for " loop-seconds " secs")
      (thread-sleep! loop-seconds))))

(def (netdata-hosts server port)
  (let* ((uri (format "~a:~a/api/v1/charts" server port))
	 (headers [
		   ["Accept" :: "*/*"]
		   ["Content-type" :: "application/json"]])
	 (results (do-get-generic uri headers))
	 (myjson (from-json results))
	 (hosts []))
    (let-hash myjson
      (for-each
	(lambda (h)
	  (let-hash h
	    (set! hosts (append hosts [ .hostname ]))))
	.hosts)
      hosts)))

(def (make-schema)
  "
CREATE TABLE dda ( host integer, last_updated bigint NOT NULL, metric integer, value real NOT NULL);
ALTER TABLE ONLY dda ADD CONSTRAINT dda_host_fkey FOREIGN KEY (host) REFERENCES hosts(id);
ALTER TABLE ONLY dda ADD CONSTRAINT dda_metric_fkey FOREIGN KEY (metric) REFERENCES metrics(id);
create table metrics(id serial primary key, name text not null);
create table hosts(id serial primary key, name text not null);
create unique index hosts_idx on hosts(name);
create unique index metrics_idx on metrics(name);
"
  (displayln "nothing here"))

(def (get-host-id db host hosts-hash)
  (let* ((query (sql-prepare db "select id from hosts where name = $1"))
	 (_ (sql-bind query host))
	 (id (sql-query query))
	 (_ (sql-finalize query)))
    (if (null? id)
      (let ((stmt3 (sql-prepare db "insert into hosts(name) values($1)")))
	(sql-bind stmt3 host)
	(sql-exec stmt3)
	(sql-finalize stmt3)
	(get-host-id db host hosts-hash))
      (begin
	(hash-put! hosts-hash host (car id))
	(car id)))))

(def (get-metric-id db metric metrics-hash)
  (let* ((query (sql-prepare db "select id from metrics where name = $1"))
	 (_ (sql-bind query metric))
	 (id (sql-query query))
	 (_ (sql-finalize query)))
    (if (null? id)
      (let ((stmt (sql-prepare db "insert into metrics(name) values($1)")))
	(sql-bind stmt metric)
	(sql-exec stmt)
	(sql-finalize stmt)
    	(get-metric-id db metric metrics-hash))
      (begin
	(hash-put! metrics-hash metric (car id))
	(car id)))))

(def (insert-netdata-bundle db host updated metric value hosts-hash metrics-hash)
  (let* ((host-id (or (hash-get hosts-hash host) (get-host-id db host hosts-hash)))
	 (metric-id (or (hash-get metrics-hash metric) (get-metric-id db metric metrics-hash)))
	 (dda (sql-prepare db "insert into dda(host, last_updated, metric, value) values($1, $2, $3, $4)"))
	 (binding (sql-bind dda host-id updated metric-id value)))
    (sql-exec dda)
    (sql-finalize dda)
    (update-host-time host-id updated db)))

(def (update-host-time host-id updated db)
  (let* ((stmt (sql-prepare db "update hosts set last_update = $1 where id = $2"))
	 (_ (sql-bind stmt updated host-id))
	 (_ (sql-exec stmt)))
    (sql-finalize stmt)))

(def (ensure-db-tables)
  (let* ((db (sql-connect postgresql-connect user: "postgres" passwd: "test"))
	 (stmt (sql-prepare db "CREATE TABLE if not exists dda( host integer, last_updated bigint NOT NULL, metric integer, value real NOT NULL)"))
	 (exec (sql-exec stmt))
	 (final (sql-finalize stmt)))
    (displayln final exec)))

(def (netdata config)
  (let* ((config (car (yaml-load config)))
	 (server (hash-get config "netdata-server"))
	 (port (hash-get config "netdata-port"))
	 (postgres-user (hash-get config "postgres-user"))
	 (postgres-db (hash-get config "postgres-db"))
	 (postgres-passwd (hash-get config "postgres-passwd"))
	 (use-local-db (hash-get config "use-local-db"))
	 (hosts (netdata-hosts server port))
	 (loop-seconds (hash-get config "loop-seconds"))
	 (dd-max-retries (or (hash-get config "dd-max-retries") 3))
	 (submit-metrics (hash-get config "metrics"))
	 (db (sql-connect postgresql-connect user: postgres-user passwd: postgres-passwd db: postgres-db))
	 (db-creates (sql-prepare db "create table datadog(host varchar(255) not null, last_updated bigint, metric_name varchar(255) not null, value real not null)"))
	 (hosts-hash (hash))
	 (mythreads [])
	 (metrics-hash (hash)))
    (ensure-db-tables)
    (for-each
      (lambda (host)
	(displayln "host: " host)
	(let ((myt
	       (spawn (lambda ()
			(submit-dp-hash
			 db
			 (hash
			  ("series"
			   (netdata-get-metrics
			    server
			    port
			    host
			    submit-metrics
			    db
			    hosts-hash
			    metrics-hash))))))))

	  (set! mythreads (cons myt mythreads))))
      hosts)
    (for-each
      (lambda (thread)
	(try
	 (thread-join! thread)
	 (catch (uncaught-exception? exn)
	   (display-exception (uncaught-exception-reason exn) (current-error-port)))))
      mythreads)
    (sql-close db)))

(def (netdata-get-metrics server port host submit-metrics db2 hosts-hash metrics-hash)
  (displayln "doing " host)
  (let* ((uri (netdata-make-uri server port host))
	 (db (sql-connect postgresql-connect user: "postgres" passwd: "lala"))
	 (cb #f)
	 (headers [
		   ["Accept" :: "*/*"]
		   ["Content-type" :: "application/json"]])
	 (reply (do-get-generic uri headers))
	 (metrics (from-json reply))
	 (hosts-hash (hash))
	 (metrics-hash (hash))
	 (series []))
    (sql-txn-begin db)
    (begin
      (hash-for-each
       (lambda (metric v)
	 (let ((good-metric (member (symbol->string metric) submit-metrics)))
	   (when #t ;; good-metric
	     (begin
	       (displayln "good metric: " metric)
	       (let-hash v
		 (hash-for-each
		  (lambda (kk vv)
		    (let-hash vv
		      (let* ((tags [
				    (format "environment:production")
				    (format "units:~a" ..units)
				    (format "context:~a" ..context)
				    ])
			     (new-series
			      (hash
			       ("metric" (format "netdata.~a.~a" metric kk))
			       ("points" [[ ..last_updated .value]])
			       ("type" "gauge")
			       ("host" host)
			       ("tags" tags))))

			;; (insert-netdata-bundle db
			;; 			   host
			;; 			   (number->string ..last_updated)
			;; 			   (format "~a.~a" metric kk)
			;; 			   (number->string (if (flonum? .value)
			;; 					     .value
			;; 					     0))
			;; 			   hosts-hash
			;; 			   metrics-hash)
			(let ((time-delta (- (time->seconds (builtin-current-time)) ..last_updated)))
			  (if (> time-delta 300)
			    (unless cb
			      (set! cb #t)
			      (try
			       (displayln (format "metric delay of ~a on host ~a for metric ~a.~a" time-delta host metric kk))
			       (catch (e)
				 (display-exception e))))))
			(set! series (append series (list new-series)))
			)))
		  .dimensions))))))
       metrics)
      (sql-txn-commit db)
      series)))

(def (netdata-make-uri server port host)
  (format "http://~a:~a/host/~a/api/v1/allmetrics?format=json" server port host))

;; old

(def (success? status)
  (and (>= status 200) (<= status 299)))

(def (print-object obj)
  #f)

(def (do-put uri headers data)
  (dp (print-curl "put" uri headers data))
  (let* ((reply (http-put uri
			  headers: headers
			  data: data))
	 (status (request-status reply))
	 (text (request-text reply)))

    (if (success? status)
      (displayln text)
      (displayln (format "Failure on post. Status:~a Text:~a~%" status text)))))

(def (do-delete uri headers params)
  (dp (print-curl "delete" uri headers params))
  (let* ((reply (http-delete uri
			     headers: headers
			     params: params))
	 (status (request-status reply))
	 (text (request-text reply)))

    (if (success? status)
      (displayln text)
      (displayln (format "Failure on delete. Status:~a Text:~a~%" status text)))))

(def (stringify-hash h)
  (let ((results []))
    (if (table? h)
      (begin
	(hash-for-each
	 (lambda (k v)
	   (set! results (append results (list (format " ~a->" k) (format "~a   " v)))))
	 h)
	(append-strings results))
      ;;        (pregexp-replace "\n" (append-strings results) "\t"))
      "N/A")))


(def (print-curl type uri headers data)
  ;;(displayln headers)
  (let ((heads "Content-type: application/json")
	(do-curl (getenv "DEBUG" #f)))
    (when do-curl
      (cond
       ((string=? type "get")
	(if (string=? "" data)
	  (displayln (format "curl -X GET -H \'~a\' ~a" heads uri))
	  (displayln (format "curl -X GET -H \'~a\' -d \'~a\' ~a" heads data uri))))
       ((string=? type "put")
	(displayln (format "curl -X PUT -H \'~a\' -d \'~a\' ~a" heads data uri)))
       ((string=? type "post")
	(displayln (format "curl -X POST -H \'~a\' -d \'~a\' ~a" heads data uri)))
       (else
	(displayln "unknown format " type))))))

(def (do-get uri)
  (print-curl "get" uri "" "")
  (let* ((reply (http-get uri))
	 (status (request-status reply))
	 (text (request-text reply)))
    (if (success? status)
      text
      (displayln (format "Error: got ~a on request. text: ~a~%" status text)))))

(def (do-post-generic uri headers data)
  (let* ((reply (http-post uri
			   headers: headers
			   data: data))
	 (status (request-status reply))
	 (text (request-text reply)))
    (dp (print-curl "post" uri headers data))
    (if (success? status)
      text
      (displayln (format "Error: Failure on a post. got ~a text: ~a~%" status text)))))

(def (do-get-generic uri headers)
  (let* ((reply (http-get uri
			  headers: headers))
	 (status (request-status reply))
	 (text (request-text reply)))
    (print-curl "get" uri "" "")
    (if (success? status)
      text
      (displayln (format "Error: got ~a on request. text: ~a~%" status text)))))

(def (usage-verb verb)
  (let ((howto (hash-get interactives verb)))
    (displayln "Wrong number of arguments. Usage is:")
    (displayln program-name " " (hash-get howto usage:))
    (exit 2)))

(def (usage)
  (displayln "Usage: datadog <verb>")
  (displayln "Verbs:")
  (for-each
    (lambda (k)
      (displayln (format "~a: ~a" k (hash-get (hash-get interactives k) description:))))
    (sort! (hash-keys interactives) string<?))
  (exit 2))

(def (nth n l)
  (if (or (> n (length l)) (< n 0))
    (error "Index out of bounds.")
    (if (eq? n 0)
      (car l)
      (nth (- n 1) (cdr l)))))

(def (float->int num)
  (inexact->exact
   (round num)))


(def (print-date date)
  (date->string date "~c"))


(def (local-get-active-metrics hostname)
  (let* ((db (sql-connect postgresql-connect user: "postgres" passwd: ""))
	 (stmt (sql-prepare db "select metric_name from dda where host = $1 and value > 0 group by 1 order by 1"))
	 (bind (sql-bind stmt hostname))
	 (metrics (sql-query stmt))
	 (final (sql-finalize stmt)))
    metrics))

(def (from-json json)
  (try
   (with-input-from-string json read-json)
   (catch (e)
     (displayln "error parsing json " e))))

(def (epoch->date epoch)
  (cond
   ((string? epoch)
    (time-utc->date (make-time time-utc 0 (string->number epoch))))
   ((flonum? epoch)
    (time-utc->date (make-time time-utc 0 (float->int epoch))))
   ((fixnum? epoch)
    (time-utc->date (make-time time-utc 0 epoch)))))

(def (date->epoch mydate)
  (string->number (date->string (string->date mydate "~Y-~m-~d ~H:~M:~S") "~s")))

(def (flatten x)
  (cond ((null? x) '())
	((pair? x) (append (flatten (car x)) (flatten (cdr x))))
	(else (list x))))

(def (data->get uri data)
  (if (table? data)
    (string-append
     uri "?"
     (string-join
      (hash-fold
       (lambda (key val r)
	 (cons
	  (string-append key "=" val) r))
       [] data) "&"))
    (displayln "not a table. got " data)))

(def (load-config)
  (let ((config (hash)))
    (hash-for-each
     (lambda (k v)
       (hash-put! config (string->symbol k) v))
     (car (yaml-load config-file)))
    (let-hash config
      (when (and .?key .?iv .?password)
	(hash-put! config 'token (get-password-from-config .key .iv .password)))
      config)))

(def (config)
  (let-hash (load-config)
    (displayln "Please enter your Postgresql user:")
    (def pg-user (read-line (current-input-port)))
    (displayln "Please enter your Postgresql password:")
    (def pg-password (read-line (current-input-port)))
    (displayln "Add the following lines to your " config-file)
    (let* ((password (read-line (current-input-port)))
	   (cipher (make-aes-256-ctr-cipher))
	   (iv (random-bytes (cipher-iv-length cipher)))
	   (key (random-bytes (cipher-key-length cipher)))
	   (encrypted-password (encrypt cipher key iv password))
	   (enc-pass-store (u8vector->base64-string encrypted-password))
	   (iv-store (u8vector->base64-string iv))
	   (key-store (u8vector->base64-string key)))
      (displayln "Add the following lines to your " config-file)
      (displayln "-----------------------------------------")
      (displayln "password: " enc-pass-store)
      (displayln "iv: " iv-store)
      (displayln "key: " key-store)
      (displayln "-----------------------------------------"))))

(def (get-password-from-config key iv password)
  (bytes->string
   (decrypt
    (make-aes-256-ctr-cipher)
    (base64-string->u8vector key)
    (base64-string->u8vector iv)
    (base64-string->u8vector password))))
