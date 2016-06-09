(defproject com.ververve/conveyare "0.2.3"
  :description "A light routing library for Kafka"
  :url "http://github.com/ververve/conveyare"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging  "0.3.1"]
                 [clj-time "0.11.0"]
                 [cheshire  "5.6.1"]
                 [prismatic/schema "1.1.0"]
                 [clout  "2.1.2"]
                 [camel-snake-kebab "0.3.2"]
                 [spootnik/kinsky "0.1.8"]]
  :deploy-repositories [["clojars" {:sign-releases false}]])
