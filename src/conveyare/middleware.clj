(ns conveyare.middleware)

(defn wrap-noop [handler]
  (fn [record]
    (handler record)))

(defn wrap-default-key-to-incoming [handler]
  (fn [record]
    (let [incoming (get record :key)
          receipt (handler record)
          outgoing (get receipt :key incoming)]
      (if (and receipt outgoing)
        (assoc receipt :key outgoing)
        receipt))))

(defn wrap-default-topic-to-incoming [handler]
  (fn [record]
    (let [incoming (get record :topic)
          receipt (handler record)
          outgoing (get receipt :topic incoming)]
      (if (and receipt outgoing)
        (assoc receipt :topic outgoing)
        receipt))))
