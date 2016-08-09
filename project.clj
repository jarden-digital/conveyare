(defproject com.ververve/conveyare "0.4.1"
  :description "A light routing library for Kafka"
  :url "http://github.com/ververve/conveyare"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging  "0.3.1"]
                 [clj-time "0.11.0"]
                 [prismatic/schema "1.1.0"]
                 [clout  "2.1.2"]
                 [spootnik/kinsky "0.1.8"]]
  :deploy-repositories [["clojars" {:sign-releases false}]]
  :profiles
  {:dev {:dependencies [[pjstadig/humane-test-output "0.7.1"]]
         :plugins [[com.jakemccrary/lein-test-refresh "0.14.0"]
                   [lein-kibit "0.1.2"]]
         :injections [(require 'pjstadig.humane-test-output)
                      (pjstadig.humane-test-output/activate!)]}})
