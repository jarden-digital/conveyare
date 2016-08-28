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

(defn fast-forward-offset-data [data]
  (loop [[o ff] ((juxt :offset :fast-forward) data)]
    (let [next (inc o)]
      (if-not (contains? ff next)
        (assoc data
               :offset o
               :fast-forward ff)
        (recur [next (disj ff next)])))))

(defn update-offset [offsets record now]
  (let [{:keys [topic partition offset]} record]
    (let [data (get-in offsets [topic partition] {:offset -1 :fast-forward #{}})
          current-offset (:offset data)]
      (if (< offset current-offset)
        offsets
        (let [advance? (or (neg? current-offset) (== offset current-offset))
              data (if advance?
                     (-> (assoc data
                               :offset (inc offset)
                               :at now)
                         fast-forward-offset-data)
                     (update data :fast-forward #(conj % (inc offset))))]
          (assoc-in
           offsets
           [topic partition]
           data))))))

(defn topic-offsets [offsets]
  (for [[topic partitions] offsets
        [partition v] partitions]
    {:topic topic
     :partition partition
     :offset (:offset v)
     :metadata ""}))

(defn create-consumer [conf topic]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        opts (merge {:bootstrap.servers servers
                     :group.id "myservice"
                     :auto.offset.reset "earliest"
                     :enable.auto.commit false
                     :session.timeout.ms 30000}
                    (get conf :consumer-ops {}))
        _ (log/debug "Starting consumer" topic "with opts" opts)
        [out ctl] (q.async/consumer
                   opts
                   (q/string-deserializer)
                   (q/string-deserializer))
        msgs-in (a/chan)]
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

(defn start
  "Start the transport system"
  [{:keys [transport topics handler router]}]
  (log/info "Starting transport" transport)
  (let [concurrency (get router :concurrency 5)
        producer (create-producer transport)
        consumers (for [topic topics]
                    [topic (create-consumer transport topic)])
        control-chan (a/chan concurrency)
        offsets (atom {})
        out-chan (a/merge (map #(:chan (second %)) consumers)
                          concurrency)]
    (a/go ; drain control messages for offsets
      (loop [sync (a/timeout 5000)]
        (let [[message ch] (a/alts! [control-chan sync])]
          (cond
            (or (= sync ch)
                (= :commit (:type message)))
            (let [current-offsets @offsets]
              (doseq [[topic consumer] consumers]
                (let [driver (:driver consumer)
                      offsets (topic-offsets
                               (select-keys current-offsets
                                            [topic]))]
                  (when (seq offsets)
                    (log/debug "Commiting offsets" topic offsets)
                    (a/>! driver {:op :commit
                                  :topic-offsets offsets}))))
              (recur (a/timeout 5000)))

            (some? message)
            (do
              (swap! offsets #(update-offset
                               %
                               message
                               (System/currentTimeMillis)))
              (recur sync))

            nil
            (log/debug "Closing control chan")))))
    {:up true
     :control control-chan
     :producer producer
     :consumers (into {} consumers)
     :out-chan out-chan}))

(defn stop
  "Stop the transport system"
  [this]
  (let [producer (:producer this)
        consumers (:consumers this)]
    (a/close! (:out-chan this))
    (a/close! (:control this))
    (a/close! (:chan producer))
    (a/put! (:driver producer) {:op :close})
    (doseq [consumer (vals consumers)]
      (a/close! (:chan consumer))
      (a/put! (:driver consumer) {:op :stop}))
    (assoc this :up false)))

(defn record-chan
  "Record channel containing merged stream of all consumed messages"
  [this]
  (:out-chan this))

(defn confirm-chan
  "Confirm channel for confirming processed topic/partition offset"
  [this]
  (:control this))

(defn commit
  "Commit all consumer offsets now, block until done.
  Can't be called in a go block"
  [this]
  (a/>!! (:control this) {:type :commit}))

(defn process-receipt!
  "Process a receipt and send an outgoing message via transport if required"
  [this receipt]
  (when (:produce receipt)
    (let [c (get-in this [:producer :chan])
          transport-record (select-keys receipt [:value :topic :key])]
      (if c
        (a/>!! c transport-record)
        (log/error "Failed to send message, transport not available")))))
