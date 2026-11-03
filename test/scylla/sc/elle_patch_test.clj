(ns scylla.sc.elle-patch-test
  "Histories from local runs (tier A, 2026-11-03): an SC partition run that
  stock Elle 0.2.5 calls :cyclic-versions although every read is consistent,
  and an eventually consistent canary that must still fail."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [elle.rw-register :as r]
            [jepsen.history :as h]
            [scylla.sc.elle-patch]))

(defn check [resource]
  (with-open [rd (io/reader (io/resource resource))]
    (r/check {:consistency-models [:strict-serializable]
              :linearizable-keys? true}
             (h/history (mapv edn/read-string (line-seq rd))
                        {:have-indices? true}))))

(deftest sc-history-is-valid
  (is (true? (:valid? (check "elle/sc-partition-pk15.edn")))))

(deftest ec-canary-still-fails
  (let [res (check "elle/ec-canary-pk4.edn")]
    (is (false? (:valid? res)))
    (is (seq (:anomaly-types res)))))
