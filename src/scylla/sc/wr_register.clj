(ns scylla.sc.wr-register
  "Elle rw-register on a Strong Consistency table. Every key is a clustering
  row of a partition, so a write-only txn is one single-partition BATCH and
  a read-only txn one SELECT ... WHERE pk = ? AND ck IN (...). Partitions
  are minted once by jepsen.independent, each with its own Elle key pool and
  100-200 txns, so a run touches many tablets and Raft groups; checking per
  partition loses nothing, because every txn is single-partition. Checked
  for strict serializability with linearizable keys: the atomicity and
  isolation of a single-partition batch, which a one-row register cannot
  see. The same workload with --no-sc, CL ONE and partitions is the canary
  and must name an anomaly."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.tests.cycle.wr :as wr]
            [scylla.sc [client :as c]
                       [db :as db]
                       ; Loaded for its side effect: fixes Elle's checker.
                       [elle-patch]]))

(def table (str db/keyspace ".wr_registers"))

(defn read-only?
  [txn]
  (= :r (ffirst txn)))

(defn txn-cql
  "The one CQL statement for a read-only or write-only txn in partition pk."
  [pk txn]
  (if (read-only? txn)
    (str "SELECT ck, v FROM " table " WHERE pk = " pk " AND ck IN ("
         (str/join ", " (map second txn)) ")")
    (str "BEGIN BATCH "
         (str/join (map (fn [[_ k v]]
                          (str "UPDATE " table " SET v = " v
                               " WHERE pk = " pk " AND ck = " k "; "))
                        txn))
         "APPLY BATCH")))

(defrecord Client [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open test node)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (c/retry-each
          (c/execute (:session conn)
                     (str "CREATE TABLE IF NOT EXISTS " table
                          " (pk int, ck int, v int, PRIMARY KEY (pk, ck))"))))))

  (invoke! [_ test op]
    (let [s        (:session conn)
          [pk txn] (:value op)]
      (c/with-errors op
        (if (read-only? txn)
          ; Driver ints must become longs: Elle compares values in Java
          ; collections, where Integer 5 is not Long 5. A missing row reads
          ; nil, which Elle takes as the key's initial version.
          (let [vs (into {} (map (fn [row] [(long (:ck row)) (long (:v row))])
                                 (c/execute s (txn-cql pk txn) (c/read-opts test))))]
            (assoc op :type :ok
                   :value (independent/tuple
                            pk (mapv (fn [[f k _]] [f k (get vs k)]) txn))))
          (do (c/execute s (txn-cql pk txn) (c/write-opts test))
              (assoc op :type :ok))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn one-statement?
  "Can Scylla run this txn as one statement: all reads or all writes, and
  no key twice (a batch shares one timestamp, so two writes to a cell would
  be ordered by value, not by position)?"
  [txn]
  (and (apply = (map first txn))
       (apply distinct? (map second txn))))

(defn generator
  "A fresh Elle generator per partition, keeping the txns Scylla can run,
  100-200 of them; partitions come from (range), 5 clients each."
  [opts]
  (independent/concurrent-generator
    5
    (range)
    (fn [_pk]
      (->> (wr/gen opts)
           (gen/filter (comp one-statement? :value))
           (gen/limit (+ 100 (rand-int 101)))))))

(defn empty-ok
  "A partition whose whole life fell inside an outage has no ok txns; Elle
  calls that :unknown, which would hide every other partition's verdict.
  It observed nothing, so it is valid."
  [chk]
  (reify checker/Checker
    (check [_ test history opts]
      (let [r (checker/check chk test history opts)]
        (cond-> r
          (and (= :unknown (:valid? r))
               (= [:empty-transaction-graph] (seq (:anomaly-types r))))
          (assoc :valid? true))))))

(defn workload
  "Options as scylla.core passes them: --key-count, --max-txn-length,
  --max-writes-per-key. Partitions and keys are never reused."
  [opts]
  {:client    (->Client (atom false) nil)
   :generator (generator opts)
   :checker   (independent/checker
                (empty-ok
                  (wr/checker {:consistency-models [:strict-serializable]
                               :linearizable-keys?  true})))})
