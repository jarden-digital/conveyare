(ns conveyare.core
  (:require [conveyare.model :as model]
            [conveyare.router :as router]
            [conveyare.transport :as transport]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.format :as tf]))

(def default-opts
  {:topics []
   :handler nil ; wrapped in middleware
   :transport {:bootstrap.servers "localhost:9092"
               :consumer-ops {:group.id "my-service"}
               :producer-ops {:compression.type "gzip"
                              :max.request.size 5000000}}})

; TODO topics are implicit from handler definition?
; TODO different topics have different middleware? middleware can detect that anyway

(defonce ^:private state
  (atom {}))

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

(defn send-message!
  [record]
  (let [t (:transport @state)]
    (transport/send-record! t record)))

(defn route-case
  "Creates a router function, that takes a Record and returns either
  a Receipt if processed, or nil if not"
  [& clauses]
  (apply router/route-case clauses))

(defn accept
  ""
  [route & args]
  (apply router/accept route args))
