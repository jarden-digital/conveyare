(defproject com.ververve/conveyare "0.4.5"
  :description "A light routing library for Kafka"
  :url "http://github.com/ververve/conveyare"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [org.clojure/tools.logging  "0.3.1"]
                 [clj-time "0.11.0"]
                 [prismatic/schema "1.1.0"]
                 [clout  "2.1.2"]
                 [spootnik/kinsky "0.1.12" :exclusions [org.apache.kafka/kafka-clients]]
                 [org.apache.kafka/kafka-clients "0.10.0.1"]]
  :deploy-repositories [["clojars" {:sign-releases false}]]
  :profiles
  {:dev {:dependencies [[pjstadig/humane-test-output "0.7.1"]
                        [org.apache.kafka/kafka_2.11 "0.10.0.1" :exclusions [org.slf4j/slf4j-api]]
                        [org.apache.curator/curator-test "2.11.0" :exclusions [log4j]]]
         :resource-paths ["env/dev/resources"]
         :plugins [[com.jakemccrary/lein-test-refresh "0.14.0"]
                   [lein-kibit "0.1.2"]]
         :injections [(require 'pjstadig.humane-test-output)
                      (pjstadig.humane-test-output/activate!)]}
   :test {:dependencies [[org.apache.kafka/kafka_2.11 "0.10.0.1" :exclusions [org.slf4j/slf4j-api]]
                         [org.apache.curator/curator-test "2.11.0" :exclusions [log4j]]]
          :resource-paths ["env/dev/resources"]}})
