(ns conveyare.core
  (:require [conveyare.router :as router]
            [conveyare.transport :as transport]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.format :as tf]))

(def default-opts
  {:topics {}
   :transport {:bootstrap.servers "localhost:9092"
               :consumer-ops {:group.id "my-service"}
               :producer-ops {:compression.type "gzip"
                              :max.request.size 5000000}}})

; TODO topics passed to kafka can be determined by routes created

(defonce ^:private state
  (atom {}))

; pass in map topic to route function
(defn start
  "Start conveyare system."
  [& opts]
  (let [opts (merge default-opts
                    (apply hash-map opts))
        t (transport/start opts)
        r (router/start opts t)]
    (swap! state merge
           {:transport t
            :router r
            :up true})
    :started))

(defn stop
  "Stop conveyare system."
  []
  (let [this @state]
    (router/stop (:router this))
    (transport/stop (:transport this))
    (swap! state assoc :up false)
    :stopped))

(defn status
  "Returns true if conveyare is up"
  []
  (:up @state))

(def ^:private time-formatter
  (tf/formatters :date-time))

(defn send-message!
  [uuid topic action data]
  (let [at (t/now)
        msg {:id (str uuid)
             :time (tf/unparse time-formatter at)
             :version "0.1.0"
             :user {:name "system"}
             :action action
             :data data}
        record {:topic topic
                :key action
                :value msg}
        c (get-in @state [:transport :producer :chan])]
    (if c
      (a/>!! c record)
      (log/error "failed to send message, transport not available"))))

(defn route-case
  "Returns a router function, that takes a message"
  [& clauses]
  (let [publish-f (fn [resp]
                    (let [id (:id resp)
                          topic (:topic resp)
                          action (:action resp)
                          data (:data resp)]
                      (send-message! id topic action data)))]
    (apply router/route-case publish-f clauses)))

(defn accept
  ""
  [route & args]
  (apply router/accept route args))
