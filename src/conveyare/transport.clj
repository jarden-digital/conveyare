(ns conveyare.transport
  (:require [clojure.core.async :as a]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [kinsky.client :as q]
            [kinsky.async :as q.async]
            [cheshire.core :as json]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [camel-snake-kebab.core :as csk]
            [clojure.string :as string])
  (:gen-class))

; TODO break Message out to middleware

(s/defschema Message
  {:id s/Str
   :time s/Str
   :version s/Str
   :user {:name s/Str}
   :action s/Str
   :data s/Any})

(s/defschema Record
  {:topic s/Str
   :key s/Str
   :value Message})

(def message-checker (s/checker Message))

(def record-checker (s/checker Record))

(defn parse-msg [s]
  (when s
    (try
      (json/parse-string s true)
      (catch java.io.IOException e nil))))

(defn create-producer [conf]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :producer-ops {})
        [pdriver pchan pctl] (q.async/producer (merge {:bootstrap.servers servers}
                                                      ops)
                                               (q/string-serializer)
                                               (q/string-serializer))
        msgs-out (a/chan)]
    (a/go ; drain and ignore control messages
          (loop [msg (a/<! pctl)]
            (when msg
              (log/warn "Producer control" msg)
              (recur (a/<! pctl)))))
    (a/go ; drain records to producer channel as json
          (loop [record (a/<! msgs-out)]
            (when record
              (let [checks (record-checker record)
                    msg-json (json/generate-string (:value record) {:key-fn csk/->camelCaseString})]
                (if (nil? checks)
                  (do
                    (log/debug "Sending" (dissoc record :value))
                    (a/>! pchan (assoc record :value msg-json)))
                  (log/warn "Can't send invalid record" record checks)))
              (recur (a/<! msgs-out)))))
    {:driver pdriver
     :chan msgs-out}))

(defn create-consumer [conf topic]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :consumer-ops {})
        [cdriver cchan cctl] (q.async/consume! (merge {:bootstrap.servers servers
                                                       :group-id "conveyare-service"}
                                                      ops)
                                               (q/string-deserializer)
                                               (q/string-deserializer)
                                               topic)
        msgs-in (a/chan)]
    (log/info "Bootstrap servers " servers)
    (a/go ; drain incoming records from consumer change
          (loop [record (a/<! cchan)]
            (if record
              (let [msg (parse-msg (:value record))
                    checks (message-checker msg)]
                (if (nil? checks)
                  (a/>! msgs-in (assoc record :value msg))
                  (log/warn "Ignoring invalid record" record checks))
                (recur (a/<! cchan)))
              (a/close! msgs-in))))
    (a/go ; drain and ignore control messages
          (loop [msg (a/<! cctl)]
            (when msg
              (log/warn "Consumer control" topic "msg" msg)
              (recur (a/<! cctl)))))
    {:driver cdriver
     :chan msgs-in}))

(defn start [opts]
  (log/info "Starting transport" opts)
  (let [kafka-conf (:transport opts)
        producer (create-producer kafka-conf)
        consumers (for [topic (keys (:topics opts))]
                    [topic (create-consumer kafka-conf topic)])]
    {:up true
     :producer producer
     :topics (into {} consumers)}))

(defn stop [this]
  (let [producer (:producer this)
        topics (vals (:topics this))]
    (a/close! (:chan producer))
    (.close! (:driver producer) 1000)
    (doseq [topic topics]
      (a/close! (:chan topic))
      (.stop! (:driver topic) 1000))
    (assoc this :up false)))
