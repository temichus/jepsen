(ns scylla.nemesis
  "All kinds of failure modes for Scylla!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [scylla [client :as client]
                    [db :as sdb]]
            [scylla.sc [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [net :as net]
                    [nemesis :as n]
                    [util :as util :refer [rand-nth-empty
                                           pprint-str]]]
            [jepsen.nemesis [time :as nt]
                            [membership :as membership]
                            [combined :as nc]]
            [jepsen.nemesis.membership.state :as ms]))

; This code for periodically recovering isn't currently used, but might come in
; handy for exploring Scylla behavior going forward.

(defn ordered-soonest-op-map
  "Takes a pair of maps wrapping operations. Each map has the following
  structure:

    :op       An operation
    :weight   An optional integer weighting.

  Returns whichever map has an operation which occurs sooner. If one map is
  nil, the other happens sooner. If one map's op is :pending, the other happens
  sooner. If one op has a lower :time, it happens sooner. If the two ops have
  equal :times, prefers the first op."
  [m1 m2]
  (condp = nil
    m1 m2
    m2 m1
    (let [op1 (:op m1)
          op2 (:op m2)]
      (condp = :pending
        op1 m2
        op2 m1
        (let [t1 (:time op1)
              t2 (:time op2)]
          (if (< t2 t1)
            m2
            m1))))))

(defrecord OrderedAny [gens]
  gen/Generator
  (op [this test ctx]
    (when-let [{:keys [op gen' i]}
               (->> gens
                    (map-indexed
                      (fn [i gen]
                        (when-let [[op gen'] (gen/op gen test ctx)]
                          {:op    op
                           :gen'  gen'
                           :i     i})))
                    (reduce ordered-soonest-op-map nil))]
      [op (OrderedAny. (assoc gens i gen'))]))

  (update [this test ctx event]
    (OrderedAny. (mapv (fn updater [gen]
                         (gen/update gen test ctx event))
                       gens))))

(defn ordered-any
  "Takes multiple generators and binds them together. Operations are taken from
  any generator, preferring earlier over later. Updates are propagated to all
  generators."
  [& gens]
  (condp = (count gens)
    0 nil
    1 (first gens)
      (OrderedAny. (vec gens))))

(defn after-time
  "Adjusts all operations from gen so that they execute no sooner than time t."
  [gen t]
  (gen/map (fn [op] (update op :time max t)) gen))

(defn after-times
  "All ops from gen at start seconds, then start + dt seconds, then start + 2dt
  seconds, etc."
  [start dt gen]
  (->> (iterate (partial + dt) start)
       (map util/secs->nanos)
       (map (partial after-time gen))))

(defn periodically-recover
  "Takes a package and modifies its generator to periodically evaluate
  final-generator."
  [pkg]
  (let [g  (:generator pkg)
        fg (:final-generator pkg)]
    (assoc pkg :generator
           (ordered-any
             (after-times 60 60
                          [(gen/log "Recovering...")
                           fg
                           (gen/sleep 20)
                           (gen/log "Recovery done, back to mischief")])
             g))))

(defn up?
  "Is a given node map up?"
  [node]
  (= :up (:status node)))

(defn merge-views-of-node
  "Merges several views of the same node map."
  [views]
  (assoc (reduce merge views)
         :status (first (sort-by {:up 2
                                  :down 1}
                                 (map :status views)))
         :state  (first (sort-by {:normal 1
                                  :moving 2
                                  :leaving 3
                                  :joining 4}
                                 (map :state views)))))

(defn add-op
  "Generates an add-node op for a membership state, if possible. The only nodes
  we can add are those flagged as `free`."
  [state]
  (when-let [node (rand-nth-empty (seq (:free state)))]
    {:type :info, :f :add-node, :value node}))

(def max-removed-nodes
  "Try not to remove/wipe more than this many nodes at once. Raft group 0
  needs a majority of voters: wiping 2 of 3 nodes (upstream's 2) left a
  cluster nobody could rejoin.
  ponytail: 1 is safe on 3 nodes; make it (quot (dec node-count) 2) when
  the hunt moves to 5 nodes and wants two concurrent removals."
  1)

(defn removed-or-free-nodes
  "Returns the set of nodes which are known to be free or are in the process of
  being removed or decommissioned."
  [state]
  (clojure.set/union (:free state)
                     (->> (:pending state)
                          (map first)
                          (filter (comp #{:remove-node} :f))
                          (map (comp :node :node :value))
                          set)
                     (->> (:pending state)
                          (map first)
                          (filter (comp #{:decommission-node} :f))
                          (map :value)
                          set)))

(defn up-nodes
  "Takes a state, and yields a collection of nodes which are part of the
  cluster, and think they're up."
  [state]
  (for [[via node-view] (:node-views state)
        node            node-view
        :when (and (= via (:node node))
                   (up? node)
                   (not (contains? (:free state) via)))]
    via))

(defn repair-op
  "Generates a repair op for a membership state, if possible."
  [state]
  (when-let [n (rand-nth-empty (up-nodes state))]
    {:type :info, :f :repair-node, :value n}))

(defn remove-op
  "Generates a remove node op for a membership state, if possible. Views are
  refreshed rarely and pauses are short, so a view rarely shows a node down:
  then we pick a live member, flagged :kill?, for invoke! to kill first."
  [state]
  ;(info "state" (pprint-str state))
  ;(info "nv" (pprint-str (:node-views state)))
  ; Don't remove if we've got too many nodes freed or being removed at once.
  (when (< (count (removed-or-free-nodes state)) max-removed-nodes)
    (when-let [v (or (rand-nth-empty
                       (for [[via node-view] (:node-views state)
                             node            node-view
                             ; We can only remove nodes `via` thinks are down
                             :when (and (not (up? node))
                                        ; And not nodes which are free. I'm
                                        ; not sure we actually want to prevent
                                        ; this, but I *think* it steers us into
                                        ; more-likely-to-succeed territory.
                                        (not (contains? (:free state)
                                                        (:node node)))
                                        ; Don't try to remove self.
                                        (not= via (:node node)))]
                         {:via via
                          :node (select-keys node [:node :id])}))
                     ; Members which think they're up, as decommission-op.
                     (let [up (for [[via node-view] (:node-views state)
                                    node            node-view
                                    :when (and (= via (:node node))
                                               (up? node)
                                               (not (contains? (:free state)
                                                               via)))]
                                (select-keys node [:node :id]))]
                       (when-let [node (rand-nth-empty up)]
                         (when-let [via (rand-nth-empty
                                          (remove #{(:node node)}
                                                  (map :node up)))]
                           {:via via, :node node, :kill? true}))))]
      {:type :info, :f :remove-node, :value v})))

(defn decommission-op
  "Generates a decommission node op for a membership state, if possible."
  [state]
  ; Don't remove too many nodes
  (when (< (count (removed-or-free-nodes state))
           max-removed-nodes)
    (when-let [n (rand-nth-empty (up-nodes state))]
      {:type  :info
       :f     :decommission-node
       :value n})))

(defn wipe-op
  "Generates a wipe op for a membership state, if possible. We can issue a wipe
  for any node which has a remove or decommission op pending. The idea is that
  sometimes this will be politely sequenced after the remove/decom, and other
  times, we'll nuke a node while it's only partway removed, but we'll generally
  avoid nuking healthy nodes."
  [state]
  (let [pending (map first (:pending state))
        nodes (concat (->> pending
                           (filter (comp #{:remove-node} :f))
                           (map (comp :node :node :value)))
                      (->> pending
                           (filter (comp #{:decommission-node} :f))
                           (map :value)))]
    (when-let [node (rand-nth-empty nodes)]
      {:type :info, :f :wipe-node, :value node})))

(defmacro with-nodetool-errors
  "Evals body, converting nodetool errors to values like :conn-refused."
  [& body]
  `(try+ ~@body
         (catch [:type :jepsen.control/nonzero-exit] e#
           (condp re-find (str (:out e#) (:err e#)) ; nodetool errors go to stderr
             #"Connection refused"         :conn-refused
             #"alive and owns this ID"     :node-considered-alive
             #"removenode is in progress"  :remove-in-progress
             #"Host ID not found"          :host-id-not-found
             #"failed to repair \d+ sub ranges" [:failed-to-repair-sub-ranges
                                                 (:out e#)]
             #"Repair job has failed"      :repair-failed
             ; Any other failure, e.g. a decommission cancelled for dead
             ; nodes: the node is still a member.
             [:nodetool-failed (str (:out e#) (:err e#))]))))

(def heal-settle-ms
  "Wait after a heal: the coordinator's failure detector must see healed nodes
  up, else it cancels the request for dead nodes (one 0.1 s after a heal
  was). Echoes on connections a partition broke time out up to ~9 s after
  it heals, marking live nodes DOWN; 5 s let that cancel a decommission and
  reject a join."
  15000)

(defn- heal-all!
  "Heals the network and SIGCONTs scylla on every node except `except` (resume
  is a no-op where it is not paused), then waits heal-settle-ms. The partition
  package's later :stop-partition is a second, harmless heal."
  [test db except]
  (info "Healing network and resuming nodes before a topology op")
  (net/heal! (:net test) test)
  (c/on-nodes test (remove #{except} (:nodes test))
              (fn [test node] (db/resume! db test node)))
  (Thread/sleep heal-settle-ms))

(defn- await-down!
  "Polls `nodetool status` on via until it shows node down, for up to 60 s.
  If it never does, the remove fails as alive."
  [test via node]
  (c/on-nodes test [via]
              (fn [test _]
                (loop [tries 60]
                  (when (and (pos? tries)
                             ; A failing nodetool ends the wait; the remove
                             ; then fails as alive and the target restarts.
                             (some #(and (= node (:node %)) (up? %))
                                   (try+ (sdb/nodetool-status test)
                                         (catch Object _ nil))))
                    (Thread/sleep 1000)
                    (recur (dec tries)))))))

(defn not-removed?
  "Is v, a remove or decommission result, one that left the node a member?
  A timed-out one may still finish, but wiping the node meanwhile leaves a
  dead normal node every later join waits for.
  ponytail: if the timed-out op finishes later, the node is gone but still
  counted a member until the next setup! rejoins it; check :view if that
  starves the nemesis."
  [v]
  (and (sequential? v)
       (or (= :timeout (last v))
           (= :nodetool-failed (first v)))))

; `free` is a set of nodes we've removed from the cluster and destroyed data
; on.
(defrecord MembershipState [db node-views view pending free faults]
  ms/State
  (setup! [this test] this)

  (teardown! [this test] this)

  (node-view [this test node]
    ; TODO: something is weird here. I think nodetool status might only return
    ; lines for SOME but not all of the cluster? Maybe? Maybe our parser is
    ; broken.
    (try+ (map (fn [n] (select-keys n [:status :state :node :id]))
               (sdb/nodetool-status test))
          (catch [:exit 1] e
            ;(info e)
            [{:node node, :status :down}])
          (catch [:exit 2] e
            ;(info e)
            [{:node node, :status :down}])
          (catch [:exit 137] e
            ; Killed
            [{:node node, :status :down}])))

  (merge-views [this test]
    ; TODO: something more clever here, like preferring node's own view of
    ; themselves? Preferring up over down? We don't actually *use* the merged
    ; view that much, because we're hunting for subjectively down nodes for
    ; removal, so this isn't super critical yet.
    (->> node-views
         (mapcat val)
         (group-by :node)
         vals
         ; Pick any representation of a given node.
         (map merge-views-of-node)
         (sort-by :node)))

  (fs [this]
    #{:add-node :remove-node :decommission-node :wipe-node
      :repair-node :pass})

  (op [this test]
    (or (->> (concat (when (faults :remove)
                       [(remove-op this) (wipe-op this) (add-op this)])
                     (when (faults :decommission)
                       [(decommission-op this) (wipe-op this) (add-op this)])
                     (when (faults :repair)
                       [(repair-op this)]))
             (remove nil?)
             rand-nth-empty)
        ; Well this is awkward. We're wrapped in a gen/mix along with the
        ; kill, pause, and partition generators. We need them to run first so
        ; we can start removing nodes. But if we return :pending, we'll be
        ; *stuck* here indefinitely: gen/mix won't move onto anyone else,
        ; because it's deterministic. Later I'm gonna go patch that, but for
        ; now I'm on the clock, and these changes to jepsen's core are
        ; already extensive enough...
        {:type :info, :f :pass}))

  (invoke! [this test {:keys [f value] :as op}]
    (assoc
      op :value
      (case f
        :pass :passed

        ; Topology ops are refused or stall while any normal node is paused or
        ; partitioned away, and they block the one nemesis worker (up to
        ; remove-timeout, ~120 s await-open for a join), so its :resume or
        ; :stop-partition never comes: heal first. A paused remove target
        ; stays paused: that's what we remove; one down only from a partition
        ; is healed and its remove fails as alive.
        ; ponytail: killed nodes stay down and still cancel these ops; with
        ; kill on, SCYLLADB-4753 blocks the drains anyway. Start them (not
        ; free ones) here if kill+membership coverage matters.
        :add-node
        ; A kill mid-join can get the join cancelled and the node banned:
        ; then wipe it and keep it free, so a later add-node retries it. A
        ; node the coordinator accepted is a member even with no CQL yet;
        ; wiping it would leave a dead normal node nothing removes. Stop it
        ; before reading the journal, so it can't be accepted after we look.
        (do (heal-all! test db nil)
            (try (c/on-nodes test [value]
                             (fn [test node]
                               (sdb/mark-journal!)
                               (sdb/enable!)
                               (sdb/rejoin-seeds! test node)
                               (db/start! db test node)
                               (sc/close! (sc/await-open test node))))
                 [:added value]
                 (catch Exception e
                   (c/on-nodes test [value] (fn [test node] (db/kill! db test node)))
                   (if (let [j (get (c/on-nodes test [value]
                                                (fn [_ _] (sdb/journal-since-mark)))
                                    value)]
                         (and (re-find #"coordinator accepted request to join|join: success" j)
                              (not (re-find sdb/join-refused j))))
                     (do (warn e value "no CQL, but joined; restarting it")
                         (c/on-nodes test [value] (fn [test node] (db/start! db test node)))
                         [:added value :no-cql])
                     (do (warn e value "join not accepted; wiping it")
                         (c/on-nodes test [value]
                                     (fn [test node]
                                       (sdb/wipe! db test node)
                                       (sdb/disable!)))
                         [:add-failed value])))))

        ; A :kill? target is live: kill it and let `via` see it down. If the
        ; remove then fails or times out, start it again: a dead member
        ; cancels every later topology op.
        :remove-node
        (let [target (:node (:node value))
              on-target (fn [f] (c/on-nodes test [target]
                                            (fn [test node] (f db test node))))
              r (do (heal-all! test db target)
                    (when (:kill? value)
                      (on-target db/kill!)
                      (await-down! test (:via value) target))
                    (with-nodetool-errors
                      (sdb/remove-node! test (:via value) (:node value))))]
          (when (and (:kill? value) (not (string? (get r 2))))
            (on-target db/start!))
          r)

        :decommission-node
        (do (heal-all! test db nil)
            (with-nodetool-errors (sdb/decommission-node! test value)))

        :wipe-node
        (do (c/on-nodes test [value]
                        (fn [test node]
                          (sdb/wipe! db test node)
                          (sdb/disable!)))
            [:wiped value])


        :repair-node
        (sdb/repair-node! test value))))

  (resolve [this test]
    this)

  (resolve-op [this test [op op']]
    (case (:f op)
      ; Trivial
      :pass this

      ; When we add a node, it's no longer free.
      :add-node
      (if (= :added (first (:value op')))
        (update this :free disj (:value op))
        this)

      ; We're done removing a node once it's free, or if we know the remove
      ; definitely failed.
      :remove-node
      (when (or (#{:conn-refused
                   :host-id-not-found
                   :node-considered-alive
                   :remove-in-progress}
                  (:value op'))
                (not-removed? (:value op'))
                (free (:node (:node (:value op)))))
        this)

      ; We're done decommissioning a node once it's free, or if we know the
      ; remove definitely failed.
      :decommission-node
      (when (or (#{:conn-refused
                   :host-id-not-found
                   :node-considered-alive}
                  (:value op'))
                (not-removed? (:value op'))
                (free (:value op)))
        this)

      ; Once wiped, we can mark this node as free.
      :wipe-node
      (update this :free conj (:value op))

      ; Repairs are immediately resolved.
      :repair-node
      this)))

(defn membership-package
  "Constructs a membership nemesis package if (:faults opts) includes :members"
  [opts]
  (let [opts (if (some #{:remove :decommission :repair} (:faults opts))
               (update opts :faults conj :membership)
               opts)]
    (when-let [pkg (-> opts
                       (assoc :membership {:state (map->MembershipState
                                                    {:db   (:db opts)
                                                     :free #{}
                                                     :faults (:faults opts)})
                                           :log-resolve-op? false
                                           :log-resolve?    true
                                           :log-node-views? false
                                           :log-view?       false})
                       membership/package)]
      ; At the end of the test, re-add everyone.
      (assoc pkg
             :final-generator
             ; Bounded: a node that never rejoins stays free, and would
             ; otherwise be re-added forever. A removed or decommissioned
             ; node is wiped first: a decommissioned one still runs, so
             ; starting it is a no-op. The wipe frees the node, which
             ; resolves its remove op, so it is re-added once.
             (gen/limit 8
               (fn final-gen [test ctx]
                 (let [state (-> pkg :nemesis :state deref)]
                   (when-let [node (first (removed-or-free-nodes state))]
                     {:type  :info
                      :f     (if ((:free state) node) :add-node :wipe-node)
                      :value node}))))
             :perf #{{:name "membership"
                      :fs   #{:add-node
                              :repair-node
                              :decommission-node
                              :remove-node}
                      :color "#278B66"}}))))

(defn package
  "Constructs a {:nemesis, :generator, :final-generator} map for the test.
  Options:

      :interval How long to wait between faults
      :db       The database we're going to manipulate.
      :faults   A set of faults, e.g. #{:kill, :pause, :partition}
      :targets  A map of options for each type of fault, e.g.
                {:partition {:targets [:majorities-ring ...]}}"
  [opts]
  (let [membership (membership-package opts)
        ; Membership last: its final re-adds must follow the others' heals.
        pkg (->> (concat (nc/nemesis-packages opts) [membership])
                 (remove nil?)
                 nc/compose-packages
                 ;periodically-recover
                 )]
    ; Just for testing membership generator behavior--we create a partition to
    ; get things started, then let it remove/wipe, then rejoin.
    ;(assoc pkg :generator
    ;       [(gen/once (fn [test ctx]
    ;                    {:type :info
    ;                     :f :start-partition
    ;                     :value (n/complete-grudge (split-at 1 (:nodes test)))}))
    ;        (gen/limit 5 (:generator membership))
    ;        (gen/once {:type :info, :f :stop-partition})
    ;        (:generator membership)])

    pkg
    ))
