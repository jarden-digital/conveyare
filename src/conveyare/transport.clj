(ns conveyare.transport
  (:require [conveyare.model :as model :refer [TransportRecord]]
            [clojure.core.async :as a]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [kinsky.client :as q]
            [kinsky.async :as q.async]
            [clojure.string :as string]))

(def record-checker (s/checker TransportRecord))

(defn create-producer [conf]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :producer-ops {})
        [in out] (q.async/producer (merge {:bootstrap.servers servers}
                                                      ops)
                                               (q/string-serializer)
                                               (q/string-serializer))
        msgs-out (a/chan)]
    (a/go ; drain and ignore control messages
      (loop [msg (a/<! out)]
        (when msg
          (log/info "Producer control" msg)
          (recur (a/<! out)))))
    (a/go ; drain records to producer channel
      (loop [record (a/<! msgs-out)]
        (when record
          (try
            (let [checks (record-checker record)]
              (if (nil? checks)
                (do
                  (log/debug "Sending" (model/describe-record record))
                  (a/>! in (assoc record :op :record)))
                (log/warn "Can't send invalid record" (model/describe-record record) checks)))
            (catch Exception e
              (log/error "Exception occured in producer for record" record e)))
          (recur (a/<! msgs-out)))))
    {:driver in
     :chan msgs-out}))

(defn create-consumer [conf topic]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :consumer-ops {})
        [out ctl] (q.async/consumer (merge {:bootstrap.servers servers
                                            :group.id "myservice"
                                            :enable.auto.commit false
                                            :session.timeout.ms 30000}
                                           ops)
                                    (q/string-deserializer)
                                    (q/string-deserializer))
        msgs-in (a/chan)
        control-chan (a/chan)
        offsets (atom {})]
    (a/put! ctl {:op :subscribe :topic topic})
    (a/go ; drain incoming records from consumer
      (loop [record (a/<! out)]
        (if record
          (do
            (case (:type record)
              :record (a/>! msgs-in (assoc record :action (:key record)))
              (log/info "Consumer control msg" record))
            (recur (a/<! out)))
          (a/close! msgs-in))))
    {:driver ctl
     :chan msgs-in}))

(defn start [{:keys [transport topics handler]}]
  (log/info "Starting transport" transport)
  (let [producer (create-producer transport)
        consumers (for [topic topics]
                    [topic (create-consumer transport topic)])]
    {:up true
     :producer producer
     :topics (into {} consumers)}))

(defn stop [this]
  (let [producer (:producer this)
        topics (vals (:topics this))]
    (a/close! (:chan producer))
    (a/put! (:driver producer) {:op :close})
    (doseq [topic topics]
      (a/close! (:chan topic))
      (a/put! (:driver topic) {:op :stop}))
    (assoc this :up false)))

(defn process-receipt! [this receipt]
  (when (:produce receipt)
    (let [c (get-in this [:producer :chan])
          transport-record (select-keys receipt [:value :topic :key])]
      (if c
        (a/>!! c transport-record)
        (log/error "Failed to send message, transport not available")))))
