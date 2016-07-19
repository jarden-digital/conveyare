(ns conveyare.middleware)

(defn wrap-default-key-to-incoming [handler]
  (fn [record]
    (let [receipt (handler record)
          incoming (get record :key)
          outgoing (get receipt :key incoming)]
      (if (and receipt outgoing)
        (assoc receipt :key outgoing)
        receipt))))

(defn wrap-default-topic-to-incoming [handler]
  (fn [record]
    (let [receipt (handler record)
          incoming (get record :topic)
          outgoing (get receipt :topic incoming)]
      (if (and receipt outgoing)
        (assoc receipt :topic outgoing)
        receipt))))
