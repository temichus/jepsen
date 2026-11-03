(ns scylla.sc.register
  "Linearizable registers on a Strong Consistency table: one row per key,
  plain UPDATE (no IF) and SELECT, checked by Knossos. The same workload on
  an eventually consistent keyspace (--no-sc, CL ONE, partitions) is the
  canary and must fail."
  (:require [jepsen
             [checker     :as checker]
             [client      :as client]
             [generator   :as gen]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.linearizable-register :as lr]
            [knossos.model :as model]
            [scylla.sc [client :as c]
                       [db :as db]]))

(def table
  "Fresh each run: scylla.sc.db drops the keyspace in setup!, so keys from
  (range) never meet rows of an earlier test."
  (str db/keyspace ".registers"))

(defrecord RegisterClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (c/retry-each
          (c/execute (:session conn)
                     (str "CREATE TABLE IF NOT EXISTS " table
                          " (id int PRIMARY KEY, v int)"))))))

  (invoke! [_ test op]
    (let [s     (:session conn)
          [k v] (:value op)]
      (c/with-errors op
        (case (:f op)
          :write (do (c/execute s (str "UPDATE " table " SET v = " v
                                       " WHERE id = " k)
                                (c/write-opts test))
                     (assoc op :type :ok))

          ; No row reads as -1 (writes are 0-4): a nil read is "unknown"
          ; to Knossos and would hide a lost write.
          :read (let [v (-> (c/execute s (str "SELECT v FROM " table
                                              " WHERE id = " k)
                                       (c/read-opts test))
                            first
                            (:v -1))]
                  (assoc op :type :ok :value (independent/tuple k v)))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn workload
  "Reads and writes over independent registers, 5 clients per key (Knossos
  is O(c!) in per-key concurrency), 100-200 ops per key, keys never reused."
  [opts]
  {:client    (->RegisterClient (atom false) nil)
   :checker   (independent/checker
                (checker/compose
                  {:linearizable (checker/linearizable {:model (model/register -1)})
                   :timeline     (timeline/html)}))
   :generator (independent/concurrent-generator
                5
                (range)
                (fn [k]
                  (->> (gen/mix [lr/r lr/w])
                       (gen/limit (+ 100 (rand-int 101)))
                       (gen/process-limit 20))))})
