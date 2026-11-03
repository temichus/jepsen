(ns scylla.sc.batch-isolation-test
  (:require [clojure.test :refer [deftest is]]
            [scylla.sc.batch-isolation :as bi]))

(deftest txn-cql-test
  (is (= "SELECT ck, v FROM jepsen_sc.batch_rows WHERE pk = 0 AND ck IN (4, 3, 5)"
         (bi/txn-cql [[:r 4 nil] [:r 3 nil] [:r 5 nil]])))
  (is (= (str "BEGIN BATCH "
              "UPDATE jepsen_sc.batch_rows SET v = 7 WHERE pk = 0 AND ck = 4; "
              "UPDATE jepsen_sc.batch_rows SET v = 7 WHERE pk = 0 AND ck = 3; "
              "APPLY BATCH")
         (bi/txn-cql [[:w 4 7] [:w 3 7]]))))

(deftest mixed-test
  (is (not (bi/mixed? [[:r 0 7] [:r 1 -7] [:r 2 7]])))
  (is (not (bi/mixed? [[:r 0 nil] [:r 1 nil]])))
  (is (bi/mixed? [[:r 0 7] [:r 1 8]]))
  (is (bi/mixed? [[:r 0 7] [:r 1 nil]])))
