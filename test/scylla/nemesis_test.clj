(ns scylla.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis.membership.state :as ms]
            [scylla.nemesis :as n]))

(deftest add-node-frees-only-on-added
  (let [s  (n/map->MembershipState {:free #{"n1"}})
        op {:f :add-node, :value "n1"}]
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1"]}]))))
    (is (= #{"n1"} (:free (ms/resolve-op s nil [op {:value [:add-failed "n1"]}]))))))
