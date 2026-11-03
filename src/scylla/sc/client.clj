(ns scylla.sc.client
  "Client plumbing for the Strong Consistency workloads (scylla.sc.*). A copy
  of scylla.client trimmed to the rules of plan §2.6:

  - a write is sent exactly once: the driver never retries (never-retry is
    hard-wired, there is no --retry option), every statement is marked
    non-idempotent, speculative execution stays off;
  - a known error becomes :fail only for reads; a write that timed out may
    have happened and is :info;
  - DDL may be retried (retry-each), nothing else;
  - no client timestamps: SC decides the order, and SC tables reject
    USING TIMESTAMP."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :as dt]
            [qbits.alia :as alia]
            [qbits.alia.policy [load-balancing :as load-balancing]]
            [qbits.hayt :as hayt]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.datastax.driver.core.exceptions NoHostAvailableException
                                                OperationTimedOutException
                                                ReadFailureException
                                                ReadTimeoutException
                                                TransportException
                                                UnavailableException
                                                WriteFailureException
                                                WriteTimeoutException)
           (com.datastax.driver.core ServerSideTimestampGenerator
                                     Statement)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetSocketAddress)))

;; Cluster and session

(defn never-retry
  "A RetryPolicy which always rethrows: the history must see every attempt."
  []
  (reify RetryPolicy
    (onReadTimeout [_ statement consistency required-responses received-responses data-retrieved? nb-retry]
      (RetryPolicy$RetryDecision/rethrow))

    (onUnavailable [_ statement consistency required-replica alive-replica nb-retry]
      (RetryPolicy$RetryDecision/rethrow))

    (onWriteTimeout [_ statement consistency write-type required-acks received-acks nb-retry]
      (RetryPolicy$RetryDecision/rethrow))

    (onRequestError [_ statement consistency exception nb-retry]
      (RetryPolicy$RetryDecision/rethrow))

    (init [_ cluster])

    (close [_])))

(defn cluster-opts
  "Alia cluster options for a connection pinned to `node`. Pure, so a test
  can inspect them."
  [node]
  {:contact-points [node]
   ; Every request goes to this node, so a partitioned node is observed as
   ; such instead of being silently routed around.
   :load-balancing-policy (load-balancing/whitelist-policy
                            (load-balancing/round-robin-policy)
                            [(InetSocketAddress. ^String node 9042)])
   ; Constant reconnects: we want to see a node come back promptly.
   :reconnection-policy {:type :constant, :constant-delay-ms 1000}
   :retry-policy (never-retry)
   :timestamp-generator ServerSideTimestampGenerator/INSTANCE})

(defn open
  "Returns a map of :cluster :session bound to the given node."
  [test node]
  (let [cluster (alia/cluster (cluster-opts node))]
    (try {:cluster cluster
          :session (alia/connect cluster)}
         (catch Throwable t
           (alia/shutdown cluster)
           (throw t)))))

(defn close!
  "Closes a connection map--both cluster and session."
  [conn]
  (alia/shutdown (:session conn))
  (alia/shutdown (:cluster conn)))

(def await-open-interval
  "How long to sleep between connection attempts, in ms"
  6000)

(defn await-open
  "Blocks until a connection is available, then returns that connection."
  [test node]
  (dt/with-retry [tries 20]
    (let [c (open test node)]
      (alia/execute (:session c) "SELECT * FROM system.peers")
      c)
    (catch NoHostAvailableException e
      (when (zero? tries)
        (throw+ {:type :jepsen.db/setup-failed
                 :msg  :await-open-timeout
                 :node node}))
      (info node "not yet available, retrying")
      (Thread/sleep await-open-interval)
      (retry (dec tries)))))

;; Statements

(defn statement
  "Turns a CQL string or a Hayt map into a Statement marked non-idempotent,
  so the driver never re-sends it on its own (alia only sets the flag when
  it is true)."
  ^Statement [query]
  (doto ^Statement (alia/query->statement (if (map? query) (hayt/->raw query) query)
                                          nil nil)
    (.setIdempotent false)))

(defn execute
  "alia/execute over `statement`. Options are alia's (:consistency etc.)."
  ([session query]
   (alia/execute session (statement query)))
  ([session query opts]
   (alia/execute session (statement query) opts)))

(defn read-opts
  "Alia options for a read: the test's --read-consistency, if any."
  [test]
  (if-let [c (:read-consistency test)] {:consistency c} {}))

(defn write-opts
  "Alia options for a write: the test's --write-consistency, if any."
  [test]
  (if-let [c (:write-consistency test)] {:consistency c} {}))

;; Errors

(defmacro remap-errors-helper
  "Basic error remapping. See remap-errors."
  [& body]
  `(try+ ~@body
         (catch NoHostAvailableException e#
           (throw+ {:type       :no-host-available
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch OperationTimedOutException e#
           (throw+ {:type       :operation-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch ReadFailureException e#
           (throw+ {:type       :read-failure
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch ReadTimeoutException e#
           (throw+ {:type       :read-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch TransportException e#
           (throw+ {:type       :transport
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch UnavailableException e#
           (throw+ {:type       :unavailable
                    :message    (.getMessage e#)
                    :definite?  true}))
         (catch WriteFailureException e#
           (throw+ {:type       :write-failure
                    :message    (.getMessage e#)
                    :definite?  false}))
         (catch WriteTimeoutException e#
           (throw+ {:type       :write-timeout
                    :message    (.getMessage e#)
                    :definite?  false}))))

(defmacro remap-errors
  "Evaluates body, catching known client errors and remapping them to Slingshot
  exceptions for ease of processing."
  [& body]
  `(try+ (remap-errors-helper ~@body)
         ; Alia sometimes wraps driver exceptions in its own ex-info; unwrap
         ; and remap the cause.
         (catch [:type :qbits.alia/execute] e#
           (remap-errors-helper (throw (:cause ~'&throw-context))))))

(defmacro slow-no-host-available
  "Introduces artificial latency for NoHostAvailableExceptions, which
  prevents us from performing a million no-op requests a second when the client
  thinks every node is down."
  [& body]
  `(try ~@body
        (catch NoHostAvailableException e#
          (Thread/sleep 2000)
          (throw e#))))

(defn known-error?
  "For use in try+ catch expressions: is this thrown object one we generated?"
  [ex]
  (and (map? ex) (contains? ex :definite?)))

(def idempotent-fs
  "The :f's whose failure is always :fail: a read never changes state. Every
  other :f is a write, and a write that errored indefinitely is :info -- it
  may have happened. Not a parameter on purpose (plan §2.6, layer 2)."
  #{:read})

(defmacro with-errors
  "Evaluates body for `op`, slowing no-host-available errors and remapping
  errors to friendly ones. A known error yields op with :type :fail if it is
  definite or op is a read, else :info."
  [op & body]
  `(try+ (remap-errors (slow-no-host-available ~@body))
         (catch known-error? e#
           (assoc ~op
                  :type (if (or (idempotent-fs (:f ~op))
                                (:definite? e#))
                          :fail
                          :info)
                  :error e#))))

;; DDL

(def retry-delay
  "Roughly how long should we wait between auto-retry attempts, in millis?"
  1000)

(defn wrap-retry
  "Takes a form and returns a form which is wrapped in remap-errors and a
  transparent retry."
  [form]
  `(dt/with-retry [tries# 10]
     (try+ (remap-errors ~form)
           (catch known-error? e#
             (when (zero? tries#)
               (throw+ e#))
             (info (pr-str (quote ~form)) "threw" e#
                   "; automatically retrying")
             (Thread/sleep (rand-int retry-delay))
             (~'retry (dec tries#))))))

(defmacro retry-each
  "Schema creation tends to fail while the cluster settles. Evaluates each
  form in order, retrying a form that throws a known Scylla error. Only for
  DDL in setup!: the forms are outside the history and must be idempotent."
  [& forms]
  (cons 'do (map wrap-retry forms)))
