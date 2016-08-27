(ns conveyare.transport-test
  (:require [conveyare.transport :as x]
            [clojure.core.async :as a]
            [clojure.test :as t :refer [deftest is]])
  (:import [java.io IOException]
           [java.net ServerSocket]
           [java.util Properties]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [kafka.server KafkaConfig KafkaServerStartable]
           [kafka.utils ZkUtils]
           kafka.admin.TopicCommand
           [org.apache.curator.test TestingServer])
  )

(defn start-server []
  (let [zk (TestingServer.)
        port (. (ServerSocket. 0) (getLocalPort))
        log-dir (. (Files/createTempDirectory "kafka" (into-array FileAttribute [])) (toFile))
        conf {"zookeeper.connect" (. zk (getConnectString))
              "broker.id" "1"
              "host.name" "localhost"
              "port" (str port)
              "log.dir" (. log-dir (getAbsolutePath))
              "log.flush.interval.messages" "1"}
        kafka (KafkaServerStartable. (KafkaConfig. conf))]
    ;; (println "About to start" kafka "using" conf)
    (. kafka (startup))
    ;; (println "Started" kafka)
    {:zk zk
     :kafka kafka
     :bootstrap.servers (str "localhost:" port)}))

(defn stop-server [server]
  (. (:kafka server) (shutdown))
  (. (:zk server) (stop))
  ;; (println "Stopped")
  )

(defn start-transport [setup]
  (let [t (x/start {:topics ["topic1"]
                    :transport {:bootstrap.servers (:bootstrap.servers setup)
                                :consumer-ops {:group-id "group1"}
                                :producer-ops {:compression.type "gzip"
                                               :max.request.size 5000000}}})]
    ;; wait for startup (lame, yes)
    (Thread/sleep 2000)
    t))

(deftest test-simple-send-receive
  (let [setup (start-server)
        transport (start-transport setup)
        chan (get-in transport [:topics "topic1" :chan])]
    (x/process-receipt! transport {:produce true
                                  :topic "topic1"
                                  :value "hi!"
                                  :key "what"})
    (let [t (a/timeout 9000)
          [v ch] (a/alts!! [chan t])]
      (is (= {:partition 0
              :topic "topic1"
              :value "hi!"
              :key "what"} (select-keys v [:partition :topic :value :key]))))
    (x/stop transport)
    (stop-server setup)))
