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
            [scylla [client :as sc]
                    [db :as sdb]]
            [slingshot.slingshot :refer [try+]]))

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
              "seconds")))

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
        (cu/grepkill! "scylla")
        (try+ (c/exec :service :scylla-server :stop)
              (catch [:exit 1] e)
              (catch [:exit 5] e))))

    db/Pause
    (pause! [_ test node]
      (c/su (cu/grepkill! :stop "/usr/bin/scylla")))

    (resume! [_ test node]
      (c/su (cu/grepkill! :cont :scylla)))))
