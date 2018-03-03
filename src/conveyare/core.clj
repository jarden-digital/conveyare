(ns conveyare.core
  (:require [conveyare.model :as model]
            [conveyare.router :as router]
            [conveyare.transport :as transport]
            [conveyare.middleware :as middleware]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(def default-opts
  {:topics []
   :topic-ops {}
   :handler nil
   :middleware nil
   :router {:concurrency 10}
   :transport {:concurrency 10
               :bootstrap.servers "localhost:9092"
               :consumer-ops {:group.id "my-service"}
               :producer-ops {:compression.type "gzip"
                              :max.request.size 5000000}}})

(letfn [(merge-in* [a b]
          (if (map? a)
            (merge-with merge-in* a b)
            b))]
  (defn merge-in
    "Merge multiple nested maps."
    [& args]
    (reduce merge-in* nil args)))

; TODO topics are implicit from handler definition?
; TODO different topics have different middleware? middleware can detect that anyway
; TODO different topics have different threads so that one doesn't overwhelm / block the other

(defonce ^:private state
  (atom {}))

(defn start
  "Start conveyare system."
  [& opts]
  (let [opts (merge-in default-opts
                       (apply hash-map opts))
        _ (log/info "Conveyare opts" opts)
        t (transport/start opts)
        r (router/start opts t)]
    (swap! state merge
           {:transport t
            :router r
            :middleware (:middleware opts)
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
        transport (:transport this)
        receipt (assoc receipt :produce true)
        middleware (or (:middleware this)
                       middleware/wrap-noop)
        processor (-> (fn [_]
                        (log/info "Produced" (model/describe-record receipt))
                        receipt)
                      middleware)]
    (transport/process-receipt! transport (processor nil))))

(defn send-request-response!
  [receipt response-chan]
  ;; TODO split send from receive paths, too complex
  ;; TODO rethink everything
  ;; TODO allow users to get to the fundamentals, or supply fundamentals, after all what is this for?
  ;; consistent logging
  ;; easy to get started (but it's not)
  ;; tools for routing, sending
  ;; wrap samp (except it doesn't, should wrap by default but allow it to be excluded)
  ;; TODO remove schema
  ;; TODO needs to register UUID for responses
  ;; TODO review pedestal architecture
  (let [tracking-id (str (java.util.UUID/randomUUID))
        this @state
        transport (:transport this)
        receipt (assoc receipt
                       :produce true
                       :response-chan response-chan ;; TODO
                       :tracking-id tracking-id
                       )
        middleware (or (:middleware this)
                       middleware/wrap-noop)
        processor (-> (fn [_]
                        (log/info "Produced" (model/describe-record receipt))
                        receipt)
                      middleware)]
    (transport/process-receipt! transport (processor nil)))
  )
