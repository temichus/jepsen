(ns scylla.sc.client-test
  "Plan §2.6, layer 1: nothing below the history may re-send a write."
  (:require [clojure.test :refer [deftest is]]
            [qbits.alia :as alia]
            [scylla.sc.client :as c])
  (:import (com.datastax.driver.core Cluster
                                     ServerSideTimestampGenerator
                                     Statement)
           (com.datastax.driver.core.policies NoSpeculativeExecutionPolicy
                                              RetryPolicy
                                              RetryPolicy$RetryDecision
                                              RetryPolicy$RetryDecision$Type)))

(deftest cluster-policies-test
  ; Cluster.build does not connect; the policies are inspectable offline.
  (let [^Cluster cluster (alia/cluster (c/cluster-opts "127.0.0.1"))
        policies (.getPolicies (.getConfiguration cluster))
        ^RetryPolicy retry (.getRetryPolicy policies)
        rethrow (fn [^RetryPolicy$RetryDecision d]
                  (= RetryPolicy$RetryDecision$Type/RETHROW (.getType d)))]
    (try
      (is (rethrow (.onReadTimeout retry nil nil 1 0 false 0)))
      (is (rethrow (.onWriteTimeout retry nil nil nil 1 0 0)))
      (is (rethrow (.onUnavailable retry nil nil 1 0 0)))
      (is (rethrow (.onRequestError retry nil nil nil 0)))
      (is (instance? NoSpeculativeExecutionPolicy
                     (.getSpeculativeExecutionPolicy policies)))
      (is (identical? ServerSideTimestampGenerator/INSTANCE
                      (.getTimestampGenerator policies)))
      (is (false? (.getDefaultIdempotence
                    (.getQueryOptions (.getConfiguration cluster)))))
      (finally (.close cluster)))))

(deftest statement-not-idempotent-test
  (doseq [q ["INSERT INTO t (k, v) VALUES (1, 2)"
             {:select :t :where [[= :k 1]]}]]
    (let [^Statement s (c/statement q)]
      (is (false? (.isIdempotent s)) (pr-str q)))))

(deftest consistency-defaults-test
  (is (= {:consistency :quorum} (c/read-opts {})))
  (is (= {:consistency :quorum} (c/write-opts {})))
  (is (= {:consistency :one} (c/write-opts {:write-consistency :one}))))

(deftest server-error-test
  ; "unknown verb" (SCYLLADB-4722) must not crash the worker.
  (let [e #(throw (com.datastax.driver.core.exceptions.ServerError.
                    (java.net.InetSocketAddress. "127.0.0.1" 9042) "unknown verb"))]
    (is (= :info (:type (c/with-errors {:f :write} (e)))))
    (is (= :fail (:type (c/with-errors {:f :read} (e)))))))
