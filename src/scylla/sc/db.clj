(ns scylla.sc.db
  "A DB for clusters somebody else installed and configured: SCT, or the
  local Docker nodes. It deletes data only of a node the membership nemesis
  left decommissioned, and then points that node's seeds at the other nodes
  in scylla.yaml; otherwise it only starts, stops, kills and pauses Scylla,
  and knows where the log is.

  Selected by --no-install-scylla (scylla.core). scylla.db stays for runs
  that install and re-form the cluster themselves."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [scylla [db :as sdb]]
            [scylla.sc [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]]))

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
  "Drops and creates the keyspace via `node`, so every run starts with no
  rows and with the consistency this run asked for (data outlives a test,
  and IF NOT EXISTS would keep the previous run's SC or EC keyspace). DDL,
  so retried while the cluster settles."
  [test node]
  (let [conn (sc/open test node)]
    (try (info "creating keyspace:" (create-keyspace-cql test))
         (sc/retry-each
           (sc/execute (:session conn) (str "DROP KEYSPACE IF EXISTS " keyspace))
           (sc/execute (:session conn) (create-keyspace-cql test)))
         (finally (sc/close! conn)))))

(def log-file
  "Scylla logs to journald on both SCT and the local nodes; we export the
  unit's journal here for Jepsen to download."
  "/var/log/scylla-jepsen.log")

(defn- signal!
  "Sends sig to the scylla server process, if any. Not cu/grepkill!: its
  pgrep -f also matches its own pipeline's child before it execs xargs, so
  xargs can SIGSTOP itself and the ssh call hangs forever. Not pkill -x
  scylla either: that also hits `scylla nodetool`."
  [sig]
  ; systemd 252 parses only upper case: -s kill fails, exit 1.
  (try+ (c/exec :systemctl :kill :--kill-whom=main :-s (str/upper-case (name sig)) :scylla-server)
        (catch [:exit 1] {:keys [err]}
          ; A stopped server's unit is often garbage-collected: not loaded.
          (when-not (re-find #"No main process|not loaded" err)
            (throw+)))))

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
        (try+ (db/start! this test node)
              (catch [:exit 1] _
                ; The membership nemesis can end a test with a node
                ; decommissioned, or banned after a cancelled join; Scylla
                ; only lets it rejoin with no data.
                ; Shutdown output can follow the line: read well past it.
                (if (re-find sdb/join-refused
                             (c/su (c/exec :journalctl :-u :scylla-server
                                           :-n 500 :--no-pager)))
                  (do (info node "left decommissioned; wiping to rejoin")
                      (sdb/wipe! this test node)
                      (sdb/rejoin-seeds! test node)
                      (db/start! this test node))
                  (throw+))))
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
                              log-file "; touch " sdb/repair-log))))
      [log-file sdb/repair-log])

    db/Process
    (start! [_ test node]
      (info node "starting ScyllaDB")
      (c/su (c/exec :service :scylla-server :start)))

    (kill! [_ test node]
      (info node "killing ScyllaDB")
      (c/su
        (signal! :kill)
        (try+ (c/exec :service :scylla-server :stop)
              (catch [:exit 1] e)
              (catch [:exit 5] e))))

    db/Pause
    (pause! [_ test node]
      (c/su (signal! :stop)))

    (resume! [_ test node]
      (c/su (signal! :cont)))))
