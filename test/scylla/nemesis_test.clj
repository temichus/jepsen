(ns scylla.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis.membership.state :as ms]
            [scylla.nemesis :as n]))

(deftest add-node-frees-only-on-added
  (let [s  (n/map->MembershipState {:free #{"n1"}})
        op {:f :add-node, :value "n1"}]
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1"]}]))))
    (is (= #{"n1"} (:free (ms/resolve-op s nil [op {:value [:add-failed "n1"]}]))))))

(deftest failed-decommission-resolves-without-freeing
  (let [s  (n/map->MembershipState {:free #{}})
        op {:f :decommission-node, :value "n2"}]
    (is (some? (ms/resolve-op s nil [op {:value [:decommissioned "n2" :timeout]}])))
    (is (some? (ms/resolve-op s nil [op {:value [:nodetool-failed "Canceled"]}])))
    (is (nil? (ms/resolve-op s nil [op {:value [:decommissioned "n2" "ok"]}])))))

(deftest timed-out-remove-resolves
  (let [s  (n/map->MembershipState {:free #{}})
        op {:f :remove-node, :value {:node {:node "n2"}}}]
    (is (some? (ms/resolve-op s nil [op {:value [:removed "n2" :timeout]}])))))
