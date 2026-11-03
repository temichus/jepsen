(ns scylla.sc.wr-register-test
  (:require [clojure.test :refer [deftest is]]
            [scylla.sc.wr-register :as wr]))

(deftest txn-cql-test
  (is (= "SELECT ck, v FROM jepsen_sc.wr_registers WHERE pk = 0 AND ck IN (3, 7)"
         (wr/txn-cql [[:r 3 nil] [:r 7 nil]])))
  (is (= (str "BEGIN BATCH "
              "UPDATE jepsen_sc.wr_registers SET v = 1 WHERE pk = 0 AND ck = 3; "
              "UPDATE jepsen_sc.wr_registers SET v = 4 WHERE pk = 0 AND ck = 7; "
              "APPLY BATCH")
         (wr/txn-cql [[:w 3 1] [:w 7 4]]))))

(deftest one-statement-test
  (is (wr/one-statement? [[:r 1 nil]]))
  (is (wr/one-statement? [[:w 1 1] [:w 2 1]]))
  (is (not (wr/one-statement? [[:r 1 nil] [:w 2 1]])))
  (is (not (wr/one-statement? [[:w 1 1] [:w 1 2]]))))
