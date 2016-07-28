(ns conveyare.model
  (:require [schema.core :as s]
            [clj-time.core :as t]
            [clj-time.format :as tf]))

(s/defschema Record
  {:topic s/Str
   (s/optional-key :key) s/Str
   :action (s/maybe s/Str)
   :body s/Any})

(s/defschema Receipt
  {:status (s/enum :ok :accepted :processed
                   :bad-request :internal-error)
   :produce s/Bool
   (s/optional-key :description) s/Str
   (s/optional-key :exception) Exception
   (s/optional-key :topic) s/Str
   (s/optional-key :action) s/Str
   (s/optional-key :key) s/Str
   (s/optional-key :body) s/Any})

(s/defschema TransportRecord
  {:topic s/Str
   (s/optional-key :key) s/Str
   :value s/Str})

(def ^:private time-formatter
  (tf/formatters :date-time))

(s/defn ok :- Receipt
  [body]
  {:status :ok
   :produce false
   :body body})

(s/defn status :- Receipt
  [status]
  {:status status
   :produce false})

(s/defn failure :- Receipt
  [status description]
  {:status status
   :produce false
   :description description})

(s/defn exception :- Receipt
  [status description ex]
  {:status status
   :produce false
   :description description
   :exception ex})

(defn describe-record [{topic :topic action :action}]
  (str topic ":" action))
