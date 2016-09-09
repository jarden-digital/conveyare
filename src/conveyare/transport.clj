(ns conveyare.transport
  (:require
   [conveyare.model :as model :refer [TransportRecord]]
   [clojure.core.async :as a]
   [schema.core :as s]
   [clojure.tools.logging :as log]
   [kinsky.client :as q]
   [kinsky.async :as qa]
   [clojure.string :as string])
  (:import
   (java.util.concurrent.atomic AtomicBoolean)
   (org.apache.kafka.common.errors WakeupException)
   (org.apache.kafka.clients.consumer CommitFailedException)))

(def record-checker (s/checker TransportRecord))

(defn safe-poll! [driver t]
  (try
    (q/poll! driver t)
    (catch WakeupException _ nil)
    (catch Exception ex (do (log/error ex "Exception during poll")
                            {:type :exception}))))

(defn safe-consumer
  [config topics kd vd]
  (let [concurrency (get config :concurrency 5)
        control-chan (a/chan (max 100 (* 10 concurrency)))
        records-chan (a/chan concurrency)
        driver (qa/make-consumer (dissoc config :concurrency) nil kd vd)
        underlying @driver
        listener (qa/channel-listener records-chan)
        stop-flag (AtomicBoolean.)]
    (q/subscribe! driver topics listener)
    (a/thread
      (loop []
        (let [result (safe-poll! driver 100)
              records (into [] qa/record-xform [result])]

          ;; send out records
          (when (seq records)
            (log/info "Polled" (:count result) "records on partitions" (:partitions result))
            (loop [[record & rest] records]
              (a/>!! records-chan record)
              (when rest
                (recur rest))))

          ;; process control
          (loop []
            (a/alt!!
              control-chan ([{:keys [op] :as control}]
                            (try
                              (log/debug "Doing control" op control)
                              (case op
                                :commit
                                (q/commit! driver (:topic-offsets control))

                                :seek
                                (.seek underlying (q/->topic-partition control) (:offset control))

                                :stop
                                (. stop-flag (set true)))
                              (catch WakeupException _ nil)
                              (catch Exception ex (log/warn ex "Control exception")))
                            (recur))
              :default nil)))
        (if (. stop-flag get)
          (do
            (log/info "Consumer stopping")
            (a/>!! records-chan {:type :eof})
            (q/close! driver)
            (a/close! records-chan))
          (recur))))
    [records-chan control-chan]))

(defn create-producer [conf]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        ops (get conf :producer-ops {})
        [in out] (qa/producer (merge {:bootstrap.servers servers}
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
            (catch Exception ex
              (log/error ex "Exception occured in producer for record" record)))
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

(defn set-offset-and-fast-forward [data offset now]
  (-> (assoc data
             :offset offset
             :at now)
      fast-forward-offset-data))

;; TODO use java hash set?
(defn update-offset [offsets record now max-outstanding]
  (let [{:keys [topic partition offset]} record]
    (let [data (get-in offsets [topic partition] {:offset -1 :fast-forward #{} :at 0})
          current-offset (:offset data)
          last-updated (:at data)]
      (if (< offset current-offset)
        offsets
        (let [advance? (or (neg? current-offset) (== offset current-offset))
              data (if advance?
                     (set-offset-and-fast-forward data (inc offset) now)
                     (let [data (update data :fast-forward #(conj % (inc offset)))
                           fast-forward (:fast-forward data)]
                       (if (and (< max-outstanding (count fast-forward))
                                (< 60000 (- now last-updated)))
                         ;; too many
                         (do
                           (log/warn "Can't guarantee consumption of record " [topic partition current-offset])
                           (set-offset-and-fast-forward data (inc current-offset) last-updated))
                         ;; still ok
                         data)))]
          (assoc-in offsets [topic partition] data))))))

(defn topic-offsets [offsets]
  (for [[topic partitions] offsets
        [partition v] partitions]
    {:topic topic
     :partition partition
     :offset (:offset v)
     :metadata ""}))

(defn create-consumer [conf topic]
  (let [servers (get conf :bootstrap.servers "localhost:9092")
        concurrency (get conf :concurrency 5)
        opts (merge {:bootstrap.servers servers
                     :group.id "myservice"
                     :auto.offset.reset "earliest"
                     :enable.auto.commit false
                     :max.poll.records (* 2 concurrency)
                     :session.timeout.ms 30000
                     :concurrency concurrency}
                    (get conf :consumer-ops {}))
        _ (log/debug "Starting consumer" topic "with opts" opts)
        [out ctl] (safe-consumer
                   opts
                   [topic]
                   (q/string-deserializer)
                   (q/string-deserializer))
        msgs-in (a/chan)]
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
  [{:keys [transport topics handler]}]
  (log/info "Starting transport" transport)
  (let [concurrency (get transport :concurrency 5)
        max-outstanding (* concurrency 100)
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
            (let [now (System/currentTimeMillis)]
              (swap! offsets #(update-offset % message now max-outstanding))
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
