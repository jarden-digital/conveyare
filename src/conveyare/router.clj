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

(def receipt-checker (s/checker model/Receipt))

(defn- action-matches [action-matcher message]
  (let [action (:action message)
        faux-request (assoc message :uri action)]
    (when action
      (clout/route-matches action-matcher faux-request))))

(defn if-action [action-matcher handler]
  (fn [message]
    (if-let [params (action-matches action-matcher message)]
      (handler (assoc message :params params)))))

(defn checker-for-option [option options]
  (s/checker
   (get options option s/Any)))

(defn receipted [val receiptf]
  (if (receipt-checker val)
    (receiptf val)
    val))

(defmacro try-receipted [form]
  `(try (receipted ~form
                   (fn [val#] (model/status :processed)))
        (catch Exception ex#
          (model/exception :internal-error "Exception occured" ex#))))

(defn receipt-output-check [checker {status :status output :output}]
  (when (= :ok status)
    (reduce (fn [_ {value :value}]
              (when-let [problem (checker value)]
                (reduced problem))) nil output)))

(defmacro endpoint [action args & body]
  ;; available options: :summary :accept :to :return
  (let [options (apply hash-map (drop-last body))
        f (last body)]
    `(let [accept-checker# (checker-for-option :accept ~options)]
       (if-action
        ~(clout/route-compile action)
        (fn [message#]
          (let [~args message#
                accept-problems# (accept-checker# (:body message#))]
            (if accept-problems#
              (model/failure :bad-request (pr-str accept-problems#))
              (try-receipted ~f))))))))

(defn- context-action [action]
  (let [re-context {:__path-info #"|/.*"}]
    (clout/route-compile (str action ":__path-info") re-context)))

(defn- remove-suffix [path suffix]
  (subs path 0 (- (count path) (count suffix))))

(defn if-context [action-matcher handler]
  (fn [message]
    (when-let [params (action-matches action-matcher message)]
      (let [action (:action message)
            path (:path-info message action)
            context (or (:context message) "")
            subpath (:__path-info params)
            params (dissoc params :__path-info)]
        (handler
         (assoc message
                :params params
                :path-info (if (= subpath "") "/" subpath)
                :context (remove-suffix action subpath)))))))

(defn routing
  [message & handlers]
  (some #(% message) handlers))

(defmacro context [actionp args & routes]
  `(if-context
    ~(context-action actionp)
    (fn [message#]
      (let [~args message#]
        (routing message# ~@routes)))))

(defmacro routes [& routes]
  `(fn [message#]
     (routing message# ~@routes)))

(defmacro reply [& body]
  (let [options (apply hash-map (drop-last body))
        f (last body)]
    ; TODO checker compilation here is pointless, as the macro expansion doesn't 'cache' it
    `(let [checker# (checker-for-option :accept ~options)
           action# (get ~options :action)
           topic# (get ~options :to)
           res# (receipted ~f
                           (fn [val#]
                             (merge
                              (model/ok val#)
                              {:produce true}
                              (when action# {:action action#})
                              (when topic# {:topic topic#}))))
           problems# (receipt-output-check checker# res#)]
       (if problems#
         (model/failure :internal-error (pr-str problems#))
         res#))))

(defmacro non-daemon-thread [& body]
  `(.start (Thread. (fn [] ~@body))))

(defn- logging-middleware [handler]
  (fn [record]
    (let [record-desc (model/describe-record record)
          _ (log/debug "Received" record-desc)
          start (. System (currentTimeMillis))
          receipt (handler record)
          end (. System (currentTimeMillis))
          duration (str (- end start) "ms")]
      (if-not receipt
        (log/debug "Dead letters" record-desc duration)
        (case (:status receipt)
          :ok (log/info "Ok"
                        record-desc "-->" (model/describe-record receipt)
                        duration)
          :accepted (log/info "Accepted" record-desc duration)
          :processed (log/info "Processed" record-desc duration)
          :bad-request (log/warn "Bad request" record-desc (:description receipt) duration)
          :internal-error (if-let [e (:exception receipt)]
                            (log/error e "Internal error" record (:description receipt) duration)
                            (log/error "Internal error" record (:description receipt) duration))
          (log/error "Internal error, unexpected receipt" record receipt duration)))
      receipt)))

(defn start [opts transport]
  (let [workers (get-in opts [:router :concurrency] 5)
        topics (:topics opts)
        handler (:handler opts)
        middleware (:middleware opts)
        processor (-> handler
                      logging-middleware
                      middleware)
        record-chan (t/record-chan transport)
        confirm-chan (t/confirm-chan transport)]
    (when (pos? (count topics))
      (log/info "Starting router for" topics "with concurrency" workers))
    (dotimes [n workers]
      (non-daemon-thread
       (loop []
         (when-let [record (a/<!! record-chan)]
           (try
             ;(log/debug "Worker thread" n "processing" (dissoc record :value))
             (let [receipt (processor record)]
               (t/process-receipt! transport receipt))
             (catch Exception ex
               (log/error "Routing exception while processing record" record)))
           ;; Currently confirm regardless of exception or not
           (a/>!! confirm-chan record)
           (recur)))
       (log/debug "Worker thread" n "exiting")))
    {:up true}))

(defn stop [router]
  (assoc router :up false))
