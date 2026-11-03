(ns scylla.sc.list-append
  "Elle list-append on a Strong Consistency table with a list<bigint>
  column. Every key is a clustering row of one partition (pk = 0); an
  append-only txn is one BATCH of UPDATE ... SET v = v + [x], a read-only
  txn one SELECT ... WHERE pk = 0 AND ck IN (...). Settles two unknowns:
  are collections accepted on SC tables, and does list order follow commit
  order. Checked for strict serializability."
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [generator :as gen]]
            [jepsen.tests.cycle.append :as append]
            [scylla.sc [client :as c]
                       [db :as db]
                       [wr-register :refer [one-statement?]]]))

(def table (str db/keyspace ".lists"))

(defn read-only?
  [txn]
  (= :r (ffirst txn)))

(defn txn-cql
  "The one CQL statement for a read-only or append-only txn."
  [txn]
  (if (read-only? txn)
    (str "SELECT ck, v FROM " table " WHERE pk = 0 AND ck IN ("
         (str/join ", " (map second txn)) ")")
    (str "BEGIN BATCH "
         (str/join (map (fn [[_ k x]]
                          (str "UPDATE " table " SET v = v + [" x "]"
                               " WHERE pk = 0 AND ck = " k "; "))
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
                          " (pk int, ck int, v list<bigint>,"
                          " PRIMARY KEY (pk, ck))"))))))

  (invoke! [_ test op]
    (let [s   (:session conn)
          txn (:value op)]
      (c/with-errors op
        (if (read-only? txn)
          ; A missing row reads nil, Elle's empty initial list.
          (let [vs (into {} (map (fn [row] [(long (:ck row))
                                            (some->> (:v row) (mapv long))])
                                 (c/execute s (txn-cql txn) (c/read-opts test))))]
            (assoc op :type :ok
                   :value (mapv (fn [[f k _]] [f k (get vs k)]) txn)))
          (do (c/execute s (txn-cql txn) (c/write-opts test))
              (assoc op :type :ok))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn workload
  "Options as scylla.core passes them: --key-count, --max-txn-length,
  --max-writes-per-key. Keys are minted by Elle and never reused."
  [opts]
  {:client    (->Client (atom false) nil)
   :generator (gen/filter (comp one-statement? :value) (append/gen opts))
   :checker   (append/checker {:consistency-models [:strict-serializable]})})
