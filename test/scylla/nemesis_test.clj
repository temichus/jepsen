(ns scylla.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis.membership.state :as ms]
            [scylla.nemesis :as n]))

(deftest add-node-frees-only-on-added
  (let [s  (n/map->MembershipState {:free #{"n1"}})
        op {:f :add-node, :value "n1"}]
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1"]}]))))
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1" :no-cql]}]))))
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
    (is (some? (ms/resolve-op s nil [op {:value [:removed "n2" :timeout]}])))
    (testing "any failure shape resolves; success waits for the wipe"
      (doseq [v [:conn-refused :repair-failed [:nodetool-failed "x"]
                 [:failed-to-repair-sub-ranges "x"] [:not-killed "n2" ["n3"]]]]
        (is (some? (ms/resolve-op s nil [op {:value v}])) (pr-str v)))
      (is (nil? (ms/resolve-op s nil [op {:value [:removed "n2" ""]}]))))))

(deftest remove-op-kills-a-live-member-when-none-is-down
  (let [up    (fn [n] {:node n, :id (str n "-id"), :status :up, :state :normal})
        views {"n1" [(up "n1") (up "n2")], "n2" [(up "n1") (up "n2")]}
        s     (n/map->MembershipState {:free #{}, :node-views views})
        v     (:value (n/remove-op s))]
    (is (:kill? v))
    (is (= (str (:node (:node v)) "-id") (:id (:node v))))
    (is (not= (:via v) (:node (:node v))))
    (testing "a joining node is never killed"
      (is (nil? (n/remove-op (assoc s :node-views
                                    {"n1" [(up "n1") (assoc (up "n2") :state :joining)]
                                     "n2" [(up "n1") (assoc (up "n2") :state :joining)]
                                     "n3" [(assoc (up "n3") :state :joining)]})))))
    (testing "a down node is removed as before"
      (let [v (:value (n/remove-op
                        (assoc-in s [:node-views "n1" 1 :status] :down)))]
        (is (= {:via "n1", :node {:node "n2", :id "n2-id"}} v))))
    (testing "not while another removal is pending"
      (is (nil? (n/remove-op
                  (assoc s :pending #{[{:f :decommission-node, :value "n1"}]})))))))

(deftest members-excludes-free-and-leaving-nodes
  (let [s (n/map->MembershipState
            {:free    #{"n1"}
             :pending #{[{:f :remove-node, :value {:node {:node "n2"}}}]
                        [{:f :decommission-node, :value "n3"}]}})]
    (is (= ["n4" "n5"] (n/members s {:nodes ["n1" "n2" "n3" "n4" "n5"]})))))
