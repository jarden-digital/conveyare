(ns conveyare.stage
  (:require [clojure.spec.alpha :as s]))

(s/def ::name string?)

(s/def ::send fn?)

(s/def ::receive fn?)

(s/def ::stage
  (s/keys :req [::name]
          :opt [::send ::receive]))
