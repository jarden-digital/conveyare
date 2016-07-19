(ns conveyare.router-test
  (:require [clojure.test :refer :all]
            [conveyare.router :as r]
            [conveyare.model :as m]
            [schema.core :as s]))

(defmacro is-exception [status class form]
  `(let [x# ~form]
     (is (= ~status (:status x#)))
     (is (= "Exception occured" (:description x#)))
     (is (= nil (:body x#)))
     (is (= ~class (class (:exception x#))))))

;; TODO test output is a record, with correct to topic, id, action, value
;; TODO is the to address based on route or response (or optionally either)?

(deftest endpoint
  (let [x (r/endpoint "/product/:pid" {{pid :pid} :params} (r/reply (str "<>" pid)))
        y (r/endpoint "/user" _ (str "uuu"))
        z (r/endpoint "/job" {body :body} (assoc body :done true))]
    (testing "bad msg container"
      (is (= nil
             (x {}))))
    (testing "non matching action"
      (is (= nil
             (x {:action "/admin/soup"})))
      (is (= nil
             (x {:action "/product/soup/drop"})))
      (is (= nil
             (x {:action "product/soup"})))
      (is (= nil
             (x {:action "/product/b1?foo=bar"}))))
    (testing "matching no arg action"
      (is (= {:status :processed
              :produce false}
             (y {:action "/user"}))))
    (testing "matching action"
      (is (= {:status :ok
              :produce true
              :body "<>soup"}
             (x {:topic "in"
                 :action "/product/soup"})))
      (is (= {:status :ok
              :produce true
              :body "<>b1"}
             (x {:topic "in"
                 :action "/product/b1"}))))
    (testing "body extraction"
      (is (= {:status :processed
              :produce false}
             (z {:action "/job"
                 :body {:q "42"}}))))))

(deftest endpoint-options
  (let [x (r/endpoint "/author/:n" {{n :n} :params}
                      :summary "Get author"
                      :accept {:foo s/Num}
                      (case n
                        "bob" (r/reply {:bar (str "?bob")})
                        "john" false))]
    (testing "happy case"
      (is (= {:status :ok
              :produce true
              :body {:bar "?bob"}}
             (x {:action "/author/bob"
                 :body {:foo 456}}))))
    (testing "accept schema"
      (is (= {:status :bad-request
              :produce false
              :description "{:foo (not (instance? java.lang.Number \"hi\"))}"}
             (x {:action "/author/sue"
                 :body {:foo "hi"}}))))
    (testing "exception"
      (is-exception :internal-error IllegalArgumentException
                    (x {:action "/author/jane"
                        :body {:foo 0}})))))

(deftest reply-or-not-and-also-nil-return
  (let [x (r/endpoint "/order/:id/add" {{id :id} :params}
                      (case id
                        "48" (r/reply 1)
                        "12" nil))]
    (is (= {:status :ok
            :produce true
            :body 1}
           (x {:topic "order-machine"
               :action "/order/48/add"
               :body {:item "pants" :quant 89}})))
    (is (= {:status :processed :produce false}
           (x {:topic "order-machine"
               :action "/order/12/add"
               :body {:item "pants" :quant 89}})))
    (is-exception :internal-error IllegalArgumentException
           (x {:topic "order-machine"
               :action "/order/111/add"
               :body {:item "pants" :quant 89}}))))

(deftest reply-with-all-options
  (let [order-add (fn [_ {q :quant}] {:total (inc q)})
        x (r/endpoint "/order/:id/add" {{id :id} :params
                                        order-line :body}
                      :accept {:item s/Str
                               :quant s/Num}
                      (r/reply :to "orders-topic" ; optional, default to originating topic
                               :action (str "/order/" id "/accepted") ; optional, default to originating action
                               :accept {:total s/Num} ; optional check
                               (order-add id order-line)))]
    (is (= {:status :ok
            :produce true
            :topic "orders-topic"
            :action "/order/48/accepted"
            :body {:total 90}}
           (x {:topic "order-machine"
               :action "/order/48/add"
               :body {:item "pants" :quant 89}})))))

(deftest router-context
  (let [x (r/context "/product/:pid" {{pid :pid} :params}
            (r/endpoint "/add" _ (str "a|" pid))
            (r/endpoint "/push/:sub" {{sub :sub} :params} (str "b|" pid "|" sub)))]
    (testing "matching actions"
      (is (= {:status :processed :produce false}
             (x {:action "/product/cheese/add"})))
      (is (= {:status :processed :produce false}
             (x {:action "/product/cheese/push/mild"}))))))

(deftest router-routes
  (let [x (r/routes
            (r/context "/a" _
                (r/endpoint "/b" _
                            (r/reply "yes")))
            (r/context "/c" _
                (r/endpoint "/d" _
                            (r/reply "no")))
            (r/endpoint "/e" _
                        (r/reply "maybe")))]
    (testing "good endpoints"
      (is (= {:status :ok
              :produce true
              :body "no"}
             (x {:topic "topic-1"
                 :action "/c/d"
                 :body {}})))
      (is (= {:status :ok
              :produce true
              :body "maybe"}
             (x {:topic "topic-2"
                 :action "/e"
                 :body {}}))))
    (testing "non endpoints"
      (is (= nil
             (x {:topic "topic-1"
                 :action "/f"
                 :body {}})))
      (is (= nil
             (x {:topic "topic-2"
                 :action "/a/z"
                 :body {}})))
      (is (= nil
             (x {:topic "topic-3"
                 :action "/a/b/i"
                 :body {}}))))))
