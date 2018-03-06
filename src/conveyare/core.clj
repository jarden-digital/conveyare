(ns conveyare.core
  (:require [conveyare.model :as model]
            [conveyare.router :as router]
            [conveyare.transport :as transport]
            [conveyare.middleware :as middleware]
            [conveyare.stage :as cs]
            [conveyare.record :as cr]
            [clojure.spec.alpha :as s]
            [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(s/def ::topic-subscriptions (s/coll-of string? :kind vector? :distinct true))

(s/def ::send-stages (s/coll-of ::cs/stage))

(s/def ::receive-stages (s/coll-of ::cs/stage))

(s/def ::setup
  (s/keys :req [::transport/setup]
          :opt [::topic-subscriptions ::send-stages ::receive-stages ::router/stage]))

;; (def default-opts
;;   {:topics []
;;    :topic-ops {}
;;    :handler nil

;;    :middleware nil
;;    :router {:concurrency 10}
;;    :transport {:concurrency 10
;;                :bootstrap.servers "localhost:9092"
;;                :consumer-ops {:group.id "my-service"}
;;                :producer-ops {:compression.type "gzip"
;;                               :max.request.size 5000000}}})

;; (letfn [(merge-in* [a b]
;;           (if (map? a)
;;             (merge-with merge-in* a b)
;;             b))]
;;   (defn merge-in
;;     "Merge multiple nested maps."
;;     [& args]
;;     (reduce merge-in* nil args)))

(defonce ^:private state
  (atom {}))

(defn start
  "Start conveyare system."
  [& setup]
  (let [_ (log/info "Conveyare setup" setup)
        t (transport/start setup)
        r (router/start setup t)]
    (swap! state merge
           {:transport t
            :router r
            :setup setup
            :up true})
    ::started))

(defn stop
  "Stop conveyare system."
  []
  (let [this @state]
    (router/stop (:router this))
    (transport/stop (:transport this))
    (swap! state assoc :up false)
    ::stopped))

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
                        (log/info "Produced" (cr/describe receipt))
                        receipt)
                      middleware)]
    (transport/write-message! transport (processor nil))))

(defn send-request-response!
  [receipt response-chan]
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
                        (log/info "Produced" (cr/describe receipt))
                        receipt)
                      middleware)]
    (transport/write-message! transport (processor nil)))
  )
