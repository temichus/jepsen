(ns scylla.sc.batch-isolation
  "Isolation of a single-partition BATCH on a Strong Consistency table,
  after scylla.write-isolation: every write is one BATCH that sets the
  same value on every row of a key range inside partition 0, every read
  is one SELECT of that range. A read must see one write's value on all
  rows, never a mix. Writes and reads of a range overlap on purpose, so
  the keys are fixed, not minted once."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [knossos.op :as op]
            [scylla.sc [client :as c]
                       [db :as db]]))

(def table (str db/keyspace ".batch_rows"))

(def key-range-size 3)
(def num-key-ranges 3)

(defn key-range
  "The keys of one random range, shuffled."
  []
  (let [lower (* (rand-int num-key-ranges) key-range-size)]
    (shuffle (range lower (+ lower key-range-size)))))

(defn writes
  "Ops like {:f :write :value [[:w 3 7] [:w 5 -7] [:w 4 7]]}: one magnitude
  per write, never repeated, with a random sign per row so that two batches
  with one timestamp cannot tie-break to the same value on every row."
  []
  (map (fn [v] {:f :write :value (mapv (fn [k] [:w k (* v (rand-nth [1 -1]))])
                                       (key-range))})
       (rest (range))))

(defn reads
  []
  (fn [_ _] {:f :read :value (mapv (fn [k] [:r k nil]) (key-range))}))

(defn txn-cql
  "The one CQL statement for an op's txn."
  [[[f] :as txn]]
  (if (= :r f)
    (str "SELECT ck, v FROM " table " WHERE pk = 0 AND ck IN ("
         (str/join ", " (map second txn)) ")")
    (str "BEGIN BATCH "
         (str/join (map (fn [[_ k v]]
                          (str "UPDATE " table " SET v = " v
                               " WHERE pk = 0 AND ck = " k "; "))
                        txn))
         "APPLY BATCH")))

(defn mixed?
  "Does this read's txn carry values of more than one write (magnitude)?
  Rows nobody wrote yet read nil, which counts as a value: a nil beside a
  number is a half-applied batch."
  [txn]
  (not (apply = (map #(some-> (nth % 2) long Math/abs) txn))))

(defn checker
  "Fails on any :ok read whose rows do not all carry the same value."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (filter op/ok?)
                      (filter (comp #{:read} :f))
                      (filter (comp mixed? :value)))]
        {:valid?      (empty? errs)
         :error-count (count errs)
         :errors      errs}))))

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
    (let [s   (:session conn)
          txn (:value op)]
      (c/with-errors op
        (case (:f op)
          :read (let [vs (into {} (map (fn [row] [(long (:ck row)) (long (:v row))])
                                       (c/execute s (txn-cql txn) (c/read-opts test))))]
                  (assoc op :type :ok
                         :value (mapv (fn [[f k _]] [f k (get vs k)]) txn)))
          :write (do (c/execute s (txn-cql txn) (c/write-opts test))
                     (assoc op :type :ok))))))

  (close! [_ _]
    (c/close! conn))

  (teardown! [_ _])

  client/Reusable
  (reusable? [_ _] true))

(defn workload
  [opts]
  {:client    (->Client (atom false) nil)
   :generator (gen/mix [(writes) (reads)])
   :checker   (checker)})
