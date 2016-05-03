(ns conveyare.router
  (:require [clojure.core.async :as a]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [clout.core :as clout]))

; TODO check param schema
; TODO route summary
; TODO generate documentation from this
; TODO make routes more performant by compiling schema checks etc upfront

(defn route-case [publish-f & clauses]
  (fn [record]
    (let [value (:value record)
          id (:id value)
          action (:action value)
          data (:data value)
          faux-request {:uri action}]
      (loop [[c & more] clauses]
        (if c
          (let [route (:route c)
                schema (get c :accept s/Any)
                f (:f c)
                match (clout/route-matches route faux-request)
                checks (s/check schema data)]
            (if match
              (if (nil? checks)
                (try
                  (log/info "Processing" action id)
                  (let [value-with-params (assoc value :params match)
                        result (f value-with-params)
                        schema (:return c)
                        to-topic (:to c)
                        checks (when schema (s/check schema result))]
                    (if (nil? checks)
                      (when (and to-topic publish-f)
                        (publish-f
                          {:id id
                           :topic to-topic
                           :action "/api-gateway/response/success"
                           :data result}))
                      (log/warn "Return schema failure" action id checks)))
                  (catch Exception e
                    (log/error "Exception" action id "processing" record e)
                    e))
                (log/warn "Accept schema failure" action id checks))
              (recur more)))
          (log/debug "Dead letters" action id))))))

(defn accept [route & args]
  (let [options (apply hash-map (drop-last args))
        f (last args)]
    (merge
      options
      {:route route
       :f f})))

(defmacro non-daemon-thread [& body]
  `(.start (Thread. (fn [] ~@body))))

(defn start [opts transport]
  (let [topics (:topics opts)
        publisher-chan (get-in transport [:publisher :chan])
        initial-inputs (into {}
                             (for [[topic router] topics]
                               [(get-in transport [topic :chan]) router]))]
    (non-daemon-thread
      (loop [inputs initial-inputs]
        (when (pos? (count inputs))
          (let [cs (keys inputs)
                [r c] (a/alts!! cs)]
            (if (nil? r)
              (recur (dissoc inputs c))
              (do
                (when-let [router (get inputs c)]
                  (router r))
                (recur inputs)))))))
    {:up true}))

(defn stop [router]
  (assoc router :up false))
