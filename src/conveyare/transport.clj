(ns conveyare.transport
  (:require [conveyare.model :as model :refer [Message Record message-checker record-checker]]
            [clojure.core.async :as a]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [kinsky.client :as q]
            [kinsky.async :as q.async]
            [cheshire.core :as json]
            [clj-time.core :as t]
            [clj-time.coerce :as tc]
            [clj-time.format :as tf]
            [camel-snake-kebab.core :as csk]
            [clojure.string :as string]))

(defn parse-msg [s]
  (when s
    (json/parse-string s true)))

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
              (log/info "Producer control" msg)
              (recur (a/<! pctl)))))
    (a/go ; drain records to producer channel as json
          (loop [record (a/<! msgs-out)]
            (when record
              (let [checks (record-checker record)
                    msg-json (json/generate-string (:value record) {:key-fn csk/->camelCaseString})]
                (if (nil? checks)
                  (do
                    (log/debug "Sending" (model/describe-record record))
                    (a/>! pchan (assoc record :value msg-json)))
                  (log/warn "Can't send invalid record" (model/describe-record record) checks)))
              (recur (a/<! msgs-out)))))
    {:driver pdriver
     :chan msgs-out}))

(defn create-consumer [conf topic]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :consumer-ops {})
        [cdriver cchan cctl] (q.async/consume! (merge {:bootstrap.servers servers
                                                       :group.id "conveyare-service"}
                                                      ops)
                                               (q/string-deserializer)
                                               (q/string-deserializer)
                                               topic)
        msgs-in (a/chan)]
    (a/go ; drain incoming records from consumer change
          (loop [record (a/<! cchan)]
            (if-not record
              (a/close! msgs-in)
              (do
                (try
                  (let [msg (parse-msg (:value record))
                        checks (message-checker msg)]
                    (if (nil? checks)
                      (a/>! msgs-in (assoc record :value msg))
                      (log/warn "Ignoring invalid record" record checks)))
                  (catch Exception e (log/error "Exception occured processing" record e)))
                (recur (a/<! cchan))))))
    (a/go ; drain and ignore control messages
          (loop [msg (a/<! cctl)]
            (when msg
              (log/info "Consumer control" topic "msg" msg)
              (recur (a/<! cctl)))))
    {:driver cdriver
     :chan msgs-in}))

(defn start [{:keys [transport topics]}]
  (log/info "Starting transport" transport)
  (let [producer (create-producer transport)
        consumers (for [topic (keys topics)]
                    [topic (create-consumer transport topic)])]
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

(defn send-record! [this record]
  (if-let [c (get-in this [:producer :chan])]
    (a/>!! c record)
    (log/error "Failed to send message, transport not available")))
