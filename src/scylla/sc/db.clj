(ns scylla.sc.db
  "A DB for clusters somebody else installed and configured: SCT, or the
  local Docker nodes. It never writes scylla.yaml and never deletes data; it
  only starts, stops, kills and pauses Scylla, and knows where the log is.

  Selected by --no-install-scylla (scylla.core). scylla.db stays for runs
  that install and re-form the cluster themselves."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [scylla [db :as sdb]]
            [scylla.sc [client :as sc]]
            [slingshot.slingshot :refer [try+]]))

(def keyspace
  "The keyspace every scylla.sc.* workload creates its tables in."
  "jepsen_sc")

(defn create-keyspace-cql
  "CREATE KEYSPACE for the workloads: NetworkTopologyStrategy, RF 3, tablets.
  Strongly consistent with --sc (the default), eventually consistent with
  --no-sc, which is the canary: the same workload must then fail."
  [test]
  (str "CREATE KEYSPACE IF NOT EXISTS " keyspace
       " WITH replication = {'class': 'NetworkTopologyStrategy',"
       " 'replication_factor': 3}"
       (when (:sc test) " AND consistency = 'global'")
       " AND tablets = {'initial': 32}"))

(defn create-keyspace!
  "Creates the keyspace via `node`. DDL, so retried while the cluster settles."
  [test node]
  (let [conn (sc/open test node)]
    (try (info "creating keyspace:" (create-keyspace-cql test))
         (sc/retry-each
           (sc/execute (:session conn) (create-keyspace-cql test)))
         (finally (sc/close! conn)))))

(def log-file
  "Scylla logs to journald on both SCT and the local nodes; we export the
  unit's journal here for Jepsen to download."
  "/var/log/scylla-jepsen.log")

(defn db
  "A Jepsen DB over an already-provisioned ScyllaDB cluster."
  []
  (reify db/DB
    (setup! [this test node]
      ; Undo a previous membership nemesis, which hides the binary.
      (sdb/enable!)
      ; First boot of a fresh cluster must be one node at a time.
      (sdb/wait-for-previous test node)
      (let [t1 (util/linear-time-nanos)]
        (db/start! this test node)
        (sc/close! (sc/await-open test node))
        (info "Scylla up in"
              (float (util/nanos->secs (- (util/linear-time-nanos) t1)))
              "seconds"))
      ; Nodes come up in order, so when the last one is up they all are,
      ; and an RF 3 keyspace with tablets can be created.
      (when (= node (last (:nodes test)))
        (create-keyspace! test node)))

    (teardown! [this test node]
      ; Config and data outlive the test. Jepsen itself kills the process
      ; before collecting logs (jepsen.core/snarf-logs!) unless the test is
      ; run with --leave-db-running; the next setup! starts it again.
      (info node "leaving ScyllaDB alone"))

    db/LogFiles
    (log-files [db test node]
      ; Jepsen collects logs before teardown!, so the export happens here.
      (meh (c/su (c/exec :sh :-c
                         (str "journalctl -u scylla-server --no-pager > "
                              log-file))))
      [log-file])

    db/Process
    (start! [_ test node]
      (info node "starting ScyllaDB")
      (c/su (c/exec :service :scylla-server :start)))

    (kill! [_ test node]
      (info node "killing ScyllaDB")
      (c/su
        (cu/grepkill! "/usr/bin/scylla")
        (try+ (c/exec :service :scylla-server :stop)
              (catch [:exit 1] e)
              (catch [:exit 5] e))))

    db/Pause
    (pause! [_ test node]
      (c/su (cu/grepkill! :stop "/usr/bin/scylla")))

    (resume! [_ test node]
      (c/su (cu/grepkill! :cont :scylla)))))
