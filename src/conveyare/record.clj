(ns conveyare.record
  (:require [clojure.spec.alpha :as s]
            [conveyare.transport :as ct]))

(s/def ::action string?)

(s/def ::body (s/nilable some?))

(s/def ::status #{:ok :accepted :processed :bad-request :internal-error})

(s/def ::send-map
  (s/keys :req [::ct/topic ::action ::body ::status]
          :opt [::ct/key ::description ::exception ::ct/produce]))

(s/def ::receive-map
  (s/keys :req [::ct/topic ::action ::body]
          :opt [::ct/key]))

(defn describe [m]
  ((juxt ::action ::ct/topic ::ct/partition ::ct/offset) m))
