(ns conveyare.middleware)

(defn receipt-output-update [f receipt]
  (when receipt
    (update receipt
            :output
            (fn [xs]
              (map f xs)))))

(defn wrap-default-key-to-incoming [handler]
  (fn [record]
    (let [receipt (handler record)
          incoming (get record :key)
          outgoing (get receipt :key incoming)]
      (if outgoing
        (receipt-output-update #(assoc % :key outgoing) receipt)
        receipt))))

(defn wrap-default-topic-to-incoming [handler]
  (fn [record]
    (let [receipt (handler record)
          incoming (get record :topic)
          outgoing (get receipt :topic incoming)]
      (if outgoing
        (receipt-output-update #(assoc % :topic outgoing) receipt)
        receipt))))
