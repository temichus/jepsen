(ns scylla.sc.wr-register-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [scylla.sc.wr-register :as wr]))

(deftest txn-cql-test
  (is (= "SELECT ck, v FROM jepsen_sc.wr_registers WHERE pk = 9 AND ck IN (3, 7)"
         (wr/txn-cql 9 [[:r 3 nil] [:r 7 nil]])))
  (is (= (str "BEGIN BATCH "
              "UPDATE jepsen_sc.wr_registers SET v = 1 WHERE pk = 0 AND ck = 3; "
              "UPDATE jepsen_sc.wr_registers SET v = 4 WHERE pk = 0 AND ck = 7; "
              "APPLY BATCH")
         (wr/txn-cql 0 [[:w 3 1] [:w 7 4]]))))

(deftest one-statement-test
  (is (wr/one-statement? [[:r 1 nil]]))
  (is (wr/one-statement? [[:w 1 1] [:w 2 1]]))
  (is (not (wr/one-statement? [[:r 1 nil] [:w 2 1]])))
  (is (not (wr/one-statement? [[:w 1 1] [:w 1 2]]))))

(defn stub [r]
  (reify checker/Checker (check [_ _ _ _] r)))

(deftest empty-ok-test
  ; Elle's verdict for a partition whose txns all failed.
  (is (true? (:valid? (checker/check
                        (wr/empty-ok (stub {:valid? :unknown
                                            :anomaly-types '(:empty-transaction-graph)}))
                        {} [] {}))))
  (is (false? (:valid? (checker/check
                         (wr/empty-ok (stub {:valid? false
                                             :anomaly-types [:G1c :empty-transaction-graph]}))
                         {} [] {})))))
