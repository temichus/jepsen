(ns scylla.db-test
  (:require [clojure.test :refer [deftest is]]
            [scylla.db :as db]))

(deftest parse-nodetool-status-line-test
  ; Current Scylla prints MiB; the 2020 format was KB. Both must parse, or the
  ; membership nemesis sees no node as up and only emits :pass.
  (doseq [line ["UN 172.20.0.2 2.11 MiB 256    ?    9789188f-9cee-456f-998f-abda2b507305 rack1"
                "UN  192.168.122.11  598.11 KB  256          ?       06e6a8ef-41e6-4c7e-8fc0-e6418cf18654  rack1"]]
    (let [m (db/parse-nodetool-status-line line)]
      (is (= :up (:status m)) line)
      (is (= :normal (:state m)) line)
      (is (= "rack1" (:rack m)) line))))

(deftest parse-excluded-node-test
  ; Scylla shows a node a removenode ignores as X; an unknown letter used to
  ; throw a parse error and blind the nemesis's node views.
  (is (= :excluded (:status (db/parse-nodetool-status-line
                              "XN 172.19.0.5 1.97 MiB 256    ?    16301599-8a49-49b2-a0e1-adcbe0b23db4 rack1")))))
