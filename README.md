# Conveyare

![](conveyare.jpg)

Conveyare is a light routing library for [Kafka](https://kafka.apache.org) based micro-services.

## Installation

Add the following dependency to your `project.clj` file:

```clj
[com.ververve/conveyare "0.4.3"]
```

## Usage

```clj
(require '[conveyare.router :refer [routes endpoint context reply]])
```

Process an incoming message, without reply:

```clj
(endpoint "/order/:id/ready" {{id :id} :params}
          :summary "Mark order as ready"
          (db/order-is-ready id))
```

Reply to an incoming message with a new message on a given topic

```clj
(endpoint "/order/:id/add" {{id :id} :params
                            order-line :body}
          :accept {:item s/Str
                   :quant s/Num}
          (reply :to "orders-topic" ; optional, default to originating topic
                 :action (str "/order/" id "/accepted") ; optional, default to originating action
                 :accept {:total s/Num} ; optional check
                 (db/order-add id order-line)))
```

Put it all together in routes context:

```clj
(def my-routes
  (routes
    (context "/order/:id" {{id :id} :params}
      (endpoint "/ready" _
        ;; as above
        )
      (endpoint "/add" {order-line :body}
        ;; as above
        ))
    (context "/other/context"
      ;; ...
      ))
```

## License

Copyright © 2016 Scott Abernethy

Distributed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
