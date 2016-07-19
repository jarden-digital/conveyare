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

(defn process-receipt [transport input-record receipt]
  (if-not receipt
    (log/debug "Dead letters" (model/describe-record input-record))
    (case (:status receipt)
      :ok (do
            (log/info "Ok" (model/describe-record input-record))
            (t/send-record! transport (select-keys receipt [:value :topic :key])))
      :accepted (log/info "Accepted" (model/describe-record input-record))
      :processed (log/info "Processed" (model/describe-record input-record))
      :bad-request (log/warn "Bad request" (model/describe-record input-record) (:description receipt))
      :internal-error (if-let [e (:exception receipt)]
                        (log/error "Internal error" (model/describe-record input-record) (:description receipt) e)
                        (log/error "Internal error" (model/describe-record input-record) (:description receipt)))
      (log/error "Internal error, unexpected receipt" receipt))))

(defn start [opts transport]
  (let [topics (:topics opts)
        handler (:handler opts)
        initial-cs (for [topic topics]
                     (get-in transport [:topics topic :chan]))]
    (when (pos? (count initial-cs))
      (log/info "Starting router for" topics))
    (non-daemon-thread
     (loop [cs initial-cs]
       (when (pos? (count cs))
         (let [[r c] (a/alts!! cs)]
           (if (nil? r)
             (recur (dissoc cs c))
             (do
               (log/debug "Received" (model/describe-record r))
               (process-receipt transport r (handler r))
               (recur cs)))))))
    {:up true}))

(defn stop [router]
  (assoc router :up false))
