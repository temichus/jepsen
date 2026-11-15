(ns scylla.nemesis
  "All kinds of failure modes for Scylla!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [clojure.string :as str]
            [scylla [client :as client]
                    [db :as sdb]]
            [scylla.sc [client :as sc]
                       [db :as sc-db]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [net :as net]
                    [nemesis :as n]
                    [util :as util :refer [rand-nth-empty
                                           pprint-str]]]
            [jepsen.control.util :as cu]
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

(defn members
  "Nodes still in the cluster: neither free nor being removed or
  decommissioned. Only these may be started: a wiped node would join."
  [state test]
  (remove (removed-or-free-nodes state) (:nodes test)))

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

(defn- normal-members
  "Members which think they're up and normal, as {:node, :id}."
  [state]
  (for [[via node-view] (:node-views state)
        node            node-view
        :when (and (= via (:node node))
                   (up? node)
                   (= :normal (:state node))
                   (not (contains? (:free state) via)))]
    (select-keys node [:node :id])))

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
                     ; Members which think they're up and normal, with ids:
                     ; killing a joiner makes it unstartable (see :add-node).
                     (let [up (normal-members state)]
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
             #"Concurrent request for removal" :remove-in-progress
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

(defn- unit-state
  "The scylla unit's state on the current node, e.g. \"active\", \"failed\"."
  []
  (try+ (c/exec :systemctl :is-active :scylla-server)
        (catch [:type :jepsen.control/nonzero-exit] e
          (str/trim (:out e)))))

(defn- active?
  "Is the scylla unit active on the current node?"
  []
  (try+ (= "active" (unit-state))
        (catch Object _ false)))

(defn- status-via
  "via's `nodetool status`, nil if nodetool fails or takes over 30 s."
  [test via]
  (get (c/on-nodes test [via]
                   (fn [test _]
                     (try+ (sdb/nodetool-status test :timeout 30)
                           (catch Object _ nil))))
       via))

(defn- listed-in
  "Does status, a `nodetool status`, list node, not excluded? Nil if status is nil
  (nodetool failed)."
  [status node]
  (when status
    (boolean (some #(and (= node (:node %))
                         (not= :excluded (:status %)))
                   status))))

(defn- listed?
  "Does via's `nodetool status` list node, not excluded? Nil if nodetool
  fails."
  [test via node]
  (listed-in (status-via test via) node))

(defn- answers
  "The `nodetool status` of every live normal member other than `except`
  and `dead` that answers."
  [state test except dead status]
  (->> (normal-members state)
       (map :node)
       (filter (set (members state test)))
       (remove (conj (set dead) except))
       (keep status)))

(defn dead-to-start
  "Which `dead` members to start. A timed-out decommission or remove counts as
  a member but may have finished since, banning the node: start only those
  every live normal member other than `except` that answers lists, not
  excluded. `status` maps a via to its `nodetool status`, nil if that fails;
  nil if none answers."
  [state test except dead status]
  (let [answers (answers state test except dead status)]
    (when (seq answers)
      (filter (fn [n] (every? #(listed-in % n) answers)) dead))))

(defn blocking-down
  "Which `dead` members block a topology op: those every answering member
  lists, excluded too (a remove placed but not done: Scylla cancels joins and
  decommissions for it); all of them if no member answers, e.g. after a kill
  :all. A dead node that any answering member doesn't list is a ghost
  (removed in Scylla, a member here) and doesn't block: a banned node never
  starts again, so it would block every later op."
  [state test except dead status]
  (when (seq dead)
    (let [answers (answers state test except dead status)]
      (if (seq answers)
        (filter (fn [n] (every? (fn [st] (some #(= n (:node %)) st)) answers))
                dead)
        dead))))

(defn- await-unit!
  "Polls the scylla unit for up to ~120 s until active (Type=notify: READY
  comes once CQL serves) or failed; returns its last state. Sleeps first:
  until a queued job runs, the unit still shows its old state."
  []
  (loop [tries 60]
    (Thread/sleep 2000)
    (let [s (unit-state)]
      (if (or (#{"active" "failed"} s) (= 1 tries))
        s
        (recur (dec tries))))))

(defn- start-and-await!
  "Starts scylla on the current node, unless disable!d, then polls its unit
  for up to ~120 s until active (Type=notify: READY comes once CQL serves) or
  failed (Restart=on-abnormal doesn't retry a failed start, e.g.
  SCYLLADB-5657). --no-block: the unit waits for READY with a 1y timeout.
  Logs failures."
  [test node]
  (try (if-not (cu/exists? sdb/scylla-bin)
         (info node "not starting ScyllaDB: disabled")
         (do (c/su (c/exec :systemctl :start :--no-block :scylla-server))
             (let [s (await-unit!)]
               (when-not (= "active" s)
                 (warn node "scylla unit" s "after start")))))
       (catch Exception e
         (warn e node "start failed"))))

(defn restart-result
  "A :restart op's result for node, from its unit state after the restart (or
  :disabled / :down if it was skipped) and its journal since. A node Scylla
  refuses (decommissioned, banned) is skipped; a crash on shutdown that
  systemd restarted from is :crashed, not :restarted."
  [node state journal]
  (let [j     (str journal)
        ; systemd's lines for a crash that printed nothing.
        crash (re-find #".*(?:Aborting on shard|Assertion.*failed|Segmentation fault|code=dumped|code=killed, status=(?:6/ABRT|11/SEGV)).*" j)
        start (re-find #".*Startup failed.*" j)]
    ; A crash first. Refused only if the startup failure says so: the
    ; coordinator's journal can mention a banned peer before failing for
    ; another reason.
    (cond (keyword? state)           [:skipped node state]
          crash                      [:crashed node crash]
          (= "failed" state)         (if (and start (re-find sdb/join-refused start))
                                       [:skipped node :refused]
                                       [:start-failed node (or start state)])
          (= "active" state)         [:restarted node]
          :else                      (cond-> [:not-up node state] start (conj start)))))

(defn- restart!
  "Cleanly restarts scylla on the current node (systemctl: SIGTERM, never
  SIGKILL) and waits ~120 s for it, unless the membership nemesis disabled
  it or its unit isn't active (killed, or a failed start: left down, for
  the kill and membership nemeses to handle). A paused node is resumed
  first: a stopped process can't act on SIGTERM. --no-block: a hung
  shutdown (TimeoutStopSec is 15 min) shows as :not-up, not a stuck
  nemesis. A failed node stays down."
  [db test node]
  (cond (not (cu/exists? sdb/scylla-bin)) (restart-result node :disabled nil)
        (not (active?))                   (restart-result node :down nil)
        :else
        (do (info node "restarting ScyllaDB")
            (db/resume! db test node)
            (sdb/mark-journal!)
            (c/su (c/exec :systemctl :restart :--no-block :scylla-server))
            (let [s (await-unit!)]
              (restart-result node s (sdb/journal-since-mark))))))

(defn restart-package
  "Clean restarts of :one, a minority, a majority or :all nodes, in parallel,
  every :interval seconds. Each op's value is a restart-result per node."
  [opts]
  (when ((:faults opts) :restart)
    {:generator (gen/stagger (:interval opts nc/default-interval)
                             (fn [_ _] {:type  :info
                                        :f     :restart
                                        :value (rand-nth [:one :minority :majority :all])}))
     :nemesis   (reify
                  n/Reflection
                  (fs [_] #{:restart})
                  n/Nemesis
                  (setup! [this _] this)
                  (invoke! [_ test op]
                    (let [db (:db opts)]
                      (->> (c/on-nodes test (nc/db-nodes test db (:value op))
                                       (partial restart! db))
                           (sort-by key)
                           (mapv val)
                           (assoc op :value))))
                  (teardown! [_ _]))
     :perf      #{{:name "restart", :fs #{:restart}, :color "#B0E9A0"}}}))

;; Replication factor changes

(def rf-timeout-ms
  "CQL timeout of an ALTER KEYSPACE, as for the membership ops. It returns
  once the topology coordinator has finished the tablet rebuilds, and any
  other topology op under way (query_processor::announce_schema_statement
  waits for wait_for_topology_not_busy). One that applies later shows in the
  next op's schema RF."
  (* 330 1000))

(defn next-rf
  "The RF to step to from rf, or nil: one lower, not below 2, or one higher,
  not above 4 nor above up-normal. Lowering ignores up-normal: after nodes went down rf may sit above
  the bound. From 3 both ways are equally likely."
  [rf up-normal]
  (rand-nth-empty (concat (when (> rf 2) [(dec rf)])
                          (when (< rf (min 4 up-normal)) [(inc rf)]))))

(defn- keyspace-rf
  "[dc rf] of the workloads' keyspace, from the schema: a timed-out ALTER may
  have applied. One DC."
  [session]
  (let [m (->> (sc/execute session (str "SELECT replication FROM system_schema.keyspaces"
                                        " WHERE keyspace_name = '" sc-db/keyspace "'"))
               first :replication (into {}))
        [dc rf] (first (dissoc m "class"))]
    [dc (Long/parseLong rf)]))

(defn- keyspace-consistency
  "The keyspace's consistency as the node's DESCRIBE shows it (its in-memory
  metadata), :none if absent, or the error if the read fails."
  [session]
  (try (let [stmt (->> (sc/execute session (str "DESCRIBE KEYSPACE " sc-db/keyspace))
                       (filter #(= "keyspace" (:type %)))
                       first :create_statement)]
         (or (second (re-find #"consistency = '(\w+)'" (str stmt))) :none))
       (catch Exception e (str e))))

(defn- alter-rf!
  "ALTER KEYSPACE to rf in dc, sent once. :ok, :timeout, or the error message."
  [session dc rf]
  (try (sc/execute session
                   (str "ALTER KEYSPACE " sc-db/keyspace " WITH replication = "
                        "{'class': 'NetworkTopologyStrategy', '" dc "': " rf "}")
                   {:read-timeout rf-timeout-ms})
       :ok
       (catch Exception e
         (let [e (if (instance? clojure.lang.ExceptionInfo e) (or (ex-cause e) e) e)]
           (if (instance? com.datastax.driver.core.exceptions.OperationTimedOutException e)
             :timeout
             (.getMessage e))))))

(defn- change-rf!
  "Steps the keyspace's RF via a node whose nodetool status answers, to the
  op's value (the final generator's 3) or else to next-rf of its up normal
  nodes."
  [test op]
  (if-let [[node status] (some #(some->> (status-via test %) (vector %))
                               (shuffle (:nodes test)))]
    (let [up-normal (count (filter #(= [:up :normal] [(:status %) (:state %)]) status))
          conn      (sc/open test node)]
      (try (let [[dc rf] (keyspace-rf (:session conn))
                 to      (or (:value op) (next-rf rf up-normal))]
             (if (or (nil? to) (= to rf))
               {:node node, :from rf, :up-normal up-normal, :result :unchanged}
               (let [t0 (System/nanoTime)
                     r  (alter-rf! (:session conn) dc to)]
                 {:node node, :from rf, :rf to, :up-normal up-normal, :result r
                  ; The ALTER has no consistency clause: Scylla must keep it.
                  :consistency (keyspace-consistency (:session conn))
                  :secs (/ (Math/round (/ (- (System/nanoTime) t0) 1e7)) 100.0)})))
           (finally (sc/close! conn))))
    {:result :no-node-answers}))

(defn rf-package
  "ALTER KEYSPACE replication factor changes between 2 and 4, one step every
  :interval seconds; the final generator returns RF to 3. RF 2 with the
  :remove fault can wedge: removing a replica at RF 2 loses the tablet
  group's majority. No guard: our mixes don't use remove."
  [opts]
  (when ((:faults opts) :rf)
    {:generator       (gen/stagger (:interval opts nc/default-interval)
                                   (fn [_ _] {:type :info, :f :rf}))
     :final-generator (gen/once {:type :info, :f :rf, :value 3})
     :nemesis         (reify
                        n/Reflection
                        (fs [_] #{:rf})
                        n/Nemesis
                        (setup! [this _] this)
                        (invoke! [_ test op]
                          ; Neither a dead node nor a failed ALTER may crash the test.
                          (assoc op :value (try (change-rf! test op)
                                                (catch Exception e
                                                  {:result (str e)}))))
                        (teardown! [_ _]))
     :perf            #{{:name "rf", :fs #{:rf}, :color "#E9C3A0"}}}))

(defn- down-members
  "Members other than except whose scylla unit isn't active."
  [test state except]
  (->> (c/on-nodes test (remove #{except} (members state test))
                   (fn [_ _] (active?)))
       (remove val)
       (map key)))

(defn- heal-all!
  "Heals the network and SIGCONTs scylla on every node except `except` (resume
  is a no-op where it is not paused), starts scylla on members except `except`
  whose unit isn't active (the kill nemesis leaves them dead) and that
  `dead-to-start` allows, then waits heal-settle-ms. The partition package's
  later :stop-partition is a second, harmless heal. Returns the members
  other than `except` still down that `blocking-down` keeps: after a kill
  :all, or with the only live member still booting, nobody answers
  `dead-to-start` and none is started."
  [test db state except]
  (info "Healing network and resuming nodes before a topology op")
  (net/heal! (:net test) test)
  (c/on-nodes test (remove #{except} (:nodes test))
              (fn [test node] (db/resume! db test node)))
  (c/on-nodes test (dead-to-start state test except
                                  (down-members test state except)
                                  #(status-via test %))
              start-and-await!)
  (Thread/sleep heal-settle-ms)
  (blocking-down state test except (down-members test state except)
                 #(status-via test %)))

(defn- await-down!
  "Polls `nodetool status` on via, for up to ~60 s, while it shows node up or
  fails. Returns via's last entry for node, nil only if node is absent; its
  :id is fresher than our views'."
  [test via node]
  (get (c/on-nodes test [via]
                   (fn [test _]
                     (loop [tries 60]
                       (let [e (->> (try+ (sdb/nodetool-status test)
                                          (catch Object _
                                            [{:node node, :status :failed}]))
                                    (filter #(= node (:node %)))
                                    first)]
                         (if (and (pos? tries)
                                  (#{:up :failed} (:status e)))
                           (do (Thread/sleep 1000)
                               (recur (dec tries)))
                           e)))))
       via))

(defn- others-down
  "Members other than node that via's `nodetool status` doesn't show up, or
  whose scylla unit isn't active: gossip takes ~20 s to see a kill, and
  meanwhile we'd kill a third voter. Via itself if nodetool fails."
  [test via node]
  (let [others (->> (get (c/on-nodes test [via]
                                     (fn [test _]
                                       (try+ (sdb/nodetool-status test)
                                             (catch Object _
                                               [{:node via, :status :failed}]))))
                         via)
                    (remove #(= node (:node %))))
        dead   (->> (c/on-nodes test (map :node others) (fn [_ _] (active?)))
                    (remove val)
                    (map key))]
    (distinct (concat (->> others (remove #(= :up (:status %))) (map :node))
                      dead))))

(defn removed?
  "Did v, a remove-node! or decommission-node! result, report success?"
  [v]
  (and (sequential? v) (string? (get v 2))))

(defn not-removed?
  "Is v, a decommission result, one that left the node a member?
  A timed-out one may still finish, but wiping the node meanwhile leaves a
  dead normal node every later join waits for. invoke! checks another
  member's status once at the timeout.
  One that finishes after that check is counted a member until it is picked
  again: that decommission fails with DECOMMISSIONED state and counts as
  done, so the node is wiped and re-added."
  [v]
  (and (sequential? v)
       (or (= :timeout (last v))
           (#{:nodetool-failed :not-decommissioned} (first v)))))

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
        ; is healed and its remove fails as alive. Killed members are started
        ; too, except the remove target.
        :add-node
        ; A kill mid-join can get the join cancelled and the node banned:
        ; then wipe it and keep it free, so a later add-node retries it. A
        ; node the coordinator accepted is a member even with no CQL yet;
        ; wiping it would leave a dead normal node nothing removes. Killing
        ; it before it reaches NORMAL makes every later start fail ("no entry
        ; for request id"), so leave an accepted one running. Otherwise stop
        ; it and read the journal again: it may have been accepted meanwhile.
        ; With a member still down the joiner hangs or fails with no raft
        ; quorum: don't try, the node stays free.
        (if-let [down (seq (heal-all! test db this nil))]
          [:add-failed value :members-down (vec down)]
          (try (c/on-nodes test [value]
                           (fn [test node]
                             (sdb/mark-journal!)
                             (sdb/enable!)
                             (sdb/rejoin-seeds! test node)
                             (db/start! db test node)
                             (sc/close! (sc/await-open test node))))
               [:added value]
               (catch Exception e
                 (let [joined? #(let [j (get (c/on-nodes test [value]
                                                         (fn [_ _] (sdb/journal-since-mark)))
                                             value)]
                                  (and (re-find #"coordinator accepted request to join|join: success" j)
                                       (not (re-find sdb/join-refused j))))]
                   (if (joined?)
                     (do (warn e value "no CQL, but joined; leaving it running")
                         [:added value :no-cql])
                     (do (c/on-nodes test [value] (fn [test node] (db/kill! db test node)))
                         (if (joined?)
                           (do (warn e value "no CQL, joined as we killed it; restarting it")
                               (try (c/on-nodes test [value] (fn [test node] (db/start! db test node)))
                                    (catch Exception e
                                      (warn e value "restart failed")))
                               [:added value :no-cql])
                           (do (warn e value "join not accepted; wiping it")
                               (c/on-nodes test [value]
                                           (fn [test node]
                                             (sdb/wipe! db test node)
                                             (sdb/disable!)))
                               [:add-failed value]))))))))

        ; A :kill? target is live: kill it and let `via` see it down. If the
        ; remove fails before its request is placed, start it again: a dead
        ; member cancels every later join and decommission. Here a timed-out
        ; remove whose target is already gone from via's status counts as
        ; removed. X (excluded) only means the request is placed: it may
        ; hang (SCYLLADB-4753) or be cancelled, and Scylla cancels joins and
        ; decommissions for the dead node meanwhile, so it stays a member,
        ; is not started (`dead-to-start`), blocks other topology ops
        ; (`blocking-down`), and a later remove-op retries it.
        ; ponytail: one that times out and finishes later leaves a member
        ; gone from every status that is never re-added, nor counted by the
        ; `max-removed-nodes` gate or `others-down`: the test runs a node
        ; short, and a later kill-and-remove can leave too few for quorum.
        ; Recognising it needs the id `via` had, not a view's.
        :remove-node
        (let [target (:node (:node value))
              via    (:via value)
              ; Only if the others still list it: the remove may have
              ; finished, banning it.
              restart! #(c/on-nodes test (dead-to-start this test nil [target]
                                                        (partial status-via test))
                                    start-and-await!)]
          (heal-all! test db this target)
          (if-not (:kill? value)
            (with-nodetool-errors (sdb/remove-node! test via (:node value)))
            ; Never kill a second member: with another one not up for via,
            ; RF=3 tablets lose quorum and every later topology op hangs.
            (if-let [down (seq (others-down test via target))]
              [:not-killed target (vec down)]
              (let [r (try
                        (c/on-nodes test [target]
                                    (fn [test node] (db/kill! db test node)))
                        (let [e (await-down! test via target)
                              r (with-nodetool-errors
                                  (sdb/remove-node!
                                    test via
                                    (merge (:node value) (select-keys e [:id]))))]
                          (if (= :timeout (get r 2))
                            (if (nil? (:status (await-down! test via target)))
                              (assoc r 2 "timed out, but gone from status")
                              r)
                            r))
                        (catch Throwable t
                          (try (restart!)
                               (catch Throwable e (.addSuppressed t e)))
                          (throw t)))]
                (when-not (removed? r) (restart!))
                r))))

        ; A timed-out decommission the coordinator finished anyway leaves
        ; the node running but banned, its group0 view frozen with every
        ; peer dead (seen after a clock step back). Used as a removenode via,
        ; it fails every remove. If another member no longer lists it, count
        ; it decommissioned, so it is wiped like any other. With another
        ; member still down it would be cancelled: don't try.
        :decommission-node
        (if-let [down (seq (remove #{value} (heal-all! test db this nil)))]
          [:not-decommissioned value (vec down)]
          (let [r   (with-nodetool-errors (sdb/decommission-node! test value))
                via (rand-nth-empty (remove #{value} (map :node (normal-members this))))]
            (cond (and (= :timeout (get r 2)) via (false? (listed? test via value)))
                  (assoc r 2 "timed out, but gone from status")

                  ; An earlier timed-out decommission finished after all.
                  (and (= :nodetool-failed (first r))
                       (re-find #"Node in DECOMMISSIONED state" (str (second r))))
                  [:decommissioned value "already decommissioned"]

                  :else r)))

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

      ; We're done removing a node once it's free, or once the remove did not
      ; report success (failed or timed out: the node is still a member).
      :remove-node
      (when (or (not (removed? (:value op')))
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
        ; Membership after the others: its final re-adds must follow their
        ; heals. RF last, back to 3 on the healed cluster.
        pkg (->> (concat (nc/nemesis-packages opts)
                         [(restart-package opts) membership (rf-package opts)])
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
