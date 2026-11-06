(ns scylla.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis.membership.state :as ms]
            [scylla.nemesis :as n]))

(deftest add-node-frees-only-on-added
  (let [s  (n/map->MembershipState {:free #{"n1"}})
        op {:f :add-node, :value "n1"}]
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1"]}]))))
    (is (= #{} (:free (ms/resolve-op s nil [op {:value [:added "n1" :no-cql]}]))))
    (is (= #{"n1"} (:free (ms/resolve-op s nil [op {:value [:add-failed "n1"]}]))))
    (is (= #{"n1"} (:free (ms/resolve-op s nil [op {:value [:add-failed "n1" :members-down ["n2"]]}]))))))

(deftest failed-decommission-resolves-without-freeing
  (let [s  (n/map->MembershipState {:free #{}})
        op {:f :decommission-node, :value "n2"}]
    (is (some? (ms/resolve-op s nil [op {:value [:decommissioned "n2" :timeout]}])))
    (is (some? (ms/resolve-op s nil [op {:value [:nodetool-failed "Canceled"]}])))
    (is (some? (ms/resolve-op s nil [op {:value [:not-decommissioned "n2" ["n3"]]}])))
    (is (not (n/removed? [:not-decommissioned "n2" ["n3"]])))
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

(deftest timed-out-decommission-counts-as-member
  (let [op  {:f :decommission-node, :value "n3"}
        op' {:value [:decommissioned "n3" :timeout]}
        s   (n/map->MembershipState {:free #{}, :pending #{[op op']}})]
    (is (empty? (n/members s {:nodes ["n3"]})))
    (is (some? (ms/resolve-op s nil [op op'])) "resolved, so dropped from :pending")
    (is (= ["n3"] (n/members (assoc s :pending #{}) {:nodes ["n3"]})))))

(deftest dead-to-start-asks-a-live-member
  (let [up    (fn [n] {:node n, :id n, :status :up, :state :normal})
        s     (n/map->MembershipState
                {:free #{}, :node-views (into {} (for [n ["n1" "n2" "n3"]]
                                                   [n [(up n)]]))})
        test  {:nodes ["n1" "n2" "n3"]}
        asked (atom [])
        st    (fn [entries] (fn [via] (swap! asked conj via) entries))]
    (testing "listed, from a live via; an unlisted one isn't started"
      (is (= ["n3"] (n/dead-to-start s test nil ["n1" "n3"]
                                     (st [{:node "n3", :status :down}]))))
      (is (= ["n2"] @asked)))
    (testing "excluded or absent: (); nodetool failed, nobody answered: nil"
      (doseq [e [[{:node "n3", :status :excluded}] []]]
        (is (= () (n/dead-to-start s test nil ["n3"] (st e))) (pr-str e)))
      (is (nil? (n/dead-to-start s test nil ["n3"] (st nil)))))
    (testing "no live via: except and dead nodes aren't asked"
      (reset! asked [])
      (is (nil? (n/dead-to-start s test "n1" ["n2" "n3"]
                                 (st [{:node "n3", :status :down}]))))
      (is (empty? @asked)))))

(deftest dead-to-start-needs-every-answer
  (let [up    (fn [n] {:node n, :id n, :status :up, :state :normal})
        s     (n/map->MembershipState
                {:free #{}, :node-views (into {} (for [n ["n1" "n2" "n3"]]
                                                   [n [(up n)]]))})
        test  {:nodes ["n1" "n2" "n3"]}
        n3    [{:node "n3", :status :down}]]
    (testing "members disagree: not started"
      (is (empty? (n/dead-to-start s test nil ["n3"] {"n1" n3, "n2" []}))))
    (testing "one fails, the other lists it: started"
      (is (= ["n3"] (n/dead-to-start s test nil ["n3"] {"n1" nil, "n2" n3}))))
    (testing "a member with a pending decommission is never asked"
      (let [asked (atom [])]
        (is (= ["n3"] (n/dead-to-start
                        (assoc s :pending #{[{:f :decommission-node, :value "n2"}]})
                        test nil ["n3"]
                        (fn [via] (swap! asked conj via) (when (= "n1" via) n3)))))
        (is (= ["n1"] @asked))))))

(deftest blocking-down-skips-ghosts
  (let [up   (fn [n] {:node n, :id n, :status :up, :state :normal})
        s    (n/map->MembershipState
               {:free #{}, :node-views (into {} (for [n ["n1" "n2" "n3"]]
                                                  [n [(up n)]]))})
        test {:nodes ["n1" "n2" "n3"]}]
    (testing "listed: blocks"
      (is (= ["n3"] (n/blocking-down s test nil ["n3"]
                                     (constantly [{:node "n3", :status :down}])))))
    (testing "a ghost, unlisted or excluded, doesn't block"
      (doseq [e [[] [{:node "n3", :status :excluded}]]]
        (is (empty? (n/blocking-down s test nil ["n3"] (constantly e))) (pr-str e)))
      (is (= () (n/blocking-down s test nil ["n3"]
                                 {"n1" [{:node "n3", :status :down}], "n2" []}))
          "members disagree"))
    (testing "nobody answers: all block"
      (is (= ["n3"] (n/blocking-down s test nil ["n3"] (constantly nil))))
      (is (= ["n2" "n3"] (n/blocking-down s test "n1" ["n2" "n3"]
                                          (constantly [])))))
    (testing "none down: nobody asked"
      (is (nil? (n/blocking-down s test nil [] #(throw (ex-info "asked" {:via %}))))))))
