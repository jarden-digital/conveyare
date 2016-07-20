(ns conveyare.core
  (:require [conveyare.model :as model]
            [conveyare.router :as router]
            [conveyare.transport :as transport]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(def default-opts
  {:topics []
   :handler nil ; wrapped in middleware
   :out-handler nil ; wrapped in middleware
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
            :out-handler (:out-handler opts)
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
  [receipt]
  (let [this @state
        t (:transport this)
        handler (:out-handler this)
        out (if handler
          (handler receipt)
          receipt)]
    (transport/send-record! t (select-keys out [:value :topic :key]))))
