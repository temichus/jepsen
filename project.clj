(defproject scylla "0.1.0"
  :description "Jepsen testing for Scylla"
  :url "http://github.com/scylladb/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.3"]
                 [org.clojure/java.jmx "0.3.1"]
                 ; Jepsen 0.3.x logs through logback 1.5, which needs slf4j 2;
                 ; metrics-core and the Scylla driver would otherwise pull 1.7.
                 [org.slf4j/slf4j-api "2.0.17"]
                 [com.github.mwiede/jsch "0.2.18"]
                 [jepsen "0.3.10" :exclusions [com.jcraft/jsch
                                              com.jcraft/jsch.agentproxy.core]]
                 [cc.qbits/alia "4.3.3" :exclusions [com.datastax.cassandra/cassandra-driver-core
                                                     com.datastax.cassandra:dse-driver]]
                 [cc.qbits/hayt "4.1.0"]
                 [com.codahale.metrics/metrics-core "3.0.2"]
                 [com.scylladb/scylla-driver-core "3.7.1-scylla-2"]]
  :main scylla.core
  :profiles {:test {:resource-paths ["test-resources"]}}
  :jvm-opts ["-Djava.awt.headless=true"
             "-Xmx6g"
             "-server"]
  :test-selectors {:steady :steady
                   :bootstrap :bootstrap
                   :map :map
                   :set :set
                   :mv :mv
                   :batch :batch
                   :lwt :lwt
                   :decommission :decommission
                   :counter :counter
                   :clock :clock
                   :all (constantly true)})
