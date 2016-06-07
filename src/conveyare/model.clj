(ns conveyare.model
  (:require [schema.core :as s]
            [clj-time.core :as t]
            [clj-time.format :as tf]))

(s/defschema Record
  {:topic s/Str
   (s/optional-key :key) s/Str
   :action s/Str
   :value s/Any})

(s/defschema OutputRecord
  {(s/optional-key :topic) s/Str
   (s/optional-key :key) s/Str
   (s/optional-key :action) s/Str
   :value s/Any})

(s/defschema Receipt
  {:status (s/enum :ok :accepted :processed
                   :bad-request :internal-error)
   (s/optional-key :description) s/Str
   (s/optional-key :exception) Exception
   :output [OutputRecord]})

(s/defschema BaseRecord
  {:topic s/Str
   :key s/Str
   :value s/Str})

(def record-checker (s/checker BaseRecord))

(def receipt-checker (s/checker Receipt))

(def ^:private time-formatter
  (tf/formatters :date-time))

(s/defn record :- Record
  [topic uuid action data]
  (let [at (t/now)]
    {:topic topic
     :key action
     :value {:id (str uuid)
             :time (tf/unparse time-formatter at)
             :version "0.1.0"
             :user {:name "system"}
             :action action
             :data data}}))

(s/defn ok :- Receipt
  [output]
  (let [output (if (vector? output) output [output])]
    {:status :ok
     :output output}))

(s/defn status :- Receipt
  [status]
  {:status status
   :output []})

(s/defn failure :- Receipt
  [status description]
  {:status status
   :description description
   :output []})

(s/defn exception :- Receipt
  [status description ex]
  {:status status
   :description description
   :exception ex
   :output []})

(defn describe-record [{topic :topic key :key}]
  (str topic " >> " key))
