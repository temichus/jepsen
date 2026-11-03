(ns scylla.sc.list-append-test
  (:require [clojure.test :refer [deftest is]]
            [scylla.sc.list-append :as la]))

(deftest txn-cql-test
  (is (= "SELECT ck, v FROM jepsen_sc.lists WHERE pk = 9 AND ck IN (3, 7)"
         (la/txn-cql 9 [[:r 3 nil] [:r 7 nil]])))
  (is (= (str "BEGIN BATCH "
              "UPDATE jepsen_sc.lists SET v = v + [1] WHERE pk = 0 AND ck = 3; "
              "UPDATE jepsen_sc.lists SET v = v + [4] WHERE pk = 0 AND ck = 7; "
              "APPLY BATCH")
         (la/txn-cql 0 [[:append 3 1] [:append 7 4]]))))
