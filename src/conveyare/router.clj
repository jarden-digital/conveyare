(ns conveyare.router
  (:require [conveyare.model :as model]
            [conveyare.transport :as t]
            [clojure.core.async :as a]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [clout.core :as clout]))

; TODO check param schema
; TODO route summary
; TODO generate documentation from this
; TODO make routes more performant by compiling schema checks etc upfront

(defn route-case [& clauses]
  (fn [record]
    (when-not (model/message-checker (:value record))
      (let [value (:value record)
            id (:id value)
            action (:action value)
            data (:data value)
            faux-request {:uri action}]
        (loop [[c & more] clauses]
          (when c
            (let [route (:route c)
                  schema (get c :accept s/Any)
                  f (:f c)
                  match (clout/route-matches route faux-request)
                  checks (s/check schema data)]
              (if match
                (if (nil? checks)
                  (try
                    (let [value-with-params (assoc value :params match)
                          result (f value-with-params)
                          schema (:return c)
                          to-topic (:to c)
                          checks (when schema (s/check schema result))]
                      (if (nil? checks)
                        (if to-topic
                          (model/ok
                            (model/record to-topic
                                          id
                                          "/api-gateway/response/success"
                                          result))
                          (model/status :processed))
                        (model/failure :internal-error
                                       (str "Return schema checks failed " checks))))
                    (catch Exception e
                      (model/exception :internal-error
                                       "Exception occured"
                                       e)))
                  (model/failure :bad-request (str "Accept schema checks failed " checks)))
                (recur more)))))))))

(defn accept [route & args]
  (let [options (apply hash-map (drop-last args))
        f (last args)]
    (merge
      options
      {:route route
       :f f})))

(defmacro non-daemon-thread [& body]
  `(.start (Thread. (fn [] ~@body))))

(defn process-receipt [transport input-record receipt]
  (if-not receipt
    (log/debug "Dead letters" (model/describe-record input-record))
    (case (:status receipt)
      :ok (do
            (log/info "Ok" (model/describe-record input-record))
            (doseq [record (:output receipt)]
            (t/send-record! transport record)))
      :accepted (log/info "Accepted" (model/describe-record input-record))
      :processed (log/info "Processed" (model/describe-record input-record))
      :bad-request (log/warn "Bad request" (model/describe-record input-record) (:description receipt))
      :internal-error (if-let [e (:exception receipt)]
                        (log/error "Internal error" (model/describe-record input-record) (:description receipt) e)
                        (log/error "Internal error" (model/describe-record input-record) (:description receipt))))))

(defn start [opts transport]
  (let [topics (:topics opts)
        initial-inputs (into {}
                             (for [[topic router] topics]
                               [(get-in transport [:topics topic :chan]) router]))]
    (when (pos? (count initial-inputs))
      (log/info "Starting router for" (keys topics)))
    (non-daemon-thread
      (loop [inputs initial-inputs]
        (when (pos? (count inputs))
          (let [cs (keys inputs)
                [r c] (a/alts!! cs)]
            (if (nil? r)
              (recur (dissoc inputs c))
              (do
                (log/debug "Received" (model/describe-record r))
                (when-let [router (get inputs c)]
                  (process-receipt transport r (router r)))
                (recur inputs)))))))
    {:up true}))

(defn stop [router]
  (assoc router :up false))
