(ns conveyare.router-test
  (:require [clojure.test :refer :all]
            [conveyare.router :as r]
            [conveyare.model :as m]
            [schema.core :as s]))

(defmacro is-exception [status class form]
  `(let [x# ~form]
     (is (= ~status (:status x#)))
     (is (= "Exception occured" (:description x#)))
     (is (= [] (:output x#)))
     (is (= ~class (class (:exception x#))))))

;; TODO test output is a record, with correct to topic, id, action, value
;; TODO is the to address based on route or response (or optionally either)?

(deftest endpoint
  (let [x (r/endpoint "/product/:pid" {{pid :pid} :params} (str "<>" pid))
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
      (is (= {:status :ok
              :output ["uuu"]}
             (y {:action "/user"}))))
    (testing "matching action"
      (is (= {:status :ok
              :output ["<>soup"]}
             (x {:action "/product/soup"})))
      (is (= {:status :ok
              :output ["<>b1"]}
             (x {:action "/product/b1"}))))
    (testing "body extraction"
      (is (= {:status :ok
              :output [{:q "42" :done true}]}
             (z {:action "/job"
                 :body {:q "42"}}))))))

(deftest endpoint-options
  (let [x (r/endpoint "/author/:n" {{n :n} :params}
                      :summary "Get author"
                      :accept {:foo s/Num}
                      :return {:bar s/Str}
                      (case n
                        "bob" {:bar (str "?bob")}
                        "john" false))]
    (testing "happy case"
      (is (= {:status :ok
              :output [{:bar "?bob"}]}
             (x {:action "/author/bob"
                 :body {:foo 456}}))))
    (testing "accept schema"
      (is (= {:status :bad-request
              :description "{:foo (not (instance? java.lang.Number \"hi\"))}"
              :output []}
             (x {:action "/author/sue"
                 :body {:foo "hi"}}))))
    (testing "return schema"
      (is (= {:status :internal-error
              :description "(not (map? false))"
              :output []}
             (x {:action "/author/john"
                 :body {:foo 12}}))))
    (testing "exception"
      (is-exception :internal-error IllegalArgumentException
                    (x {:action "/author/jane"
                        :body {:foo 0}})))))

(deftest router-context
  (let [x (r/context "/product/:pid" {{pid :pid} :params}
            (r/endpoint "/add" _ (str "a|" pid))
            (r/endpoint "/push/:sub" {{sub :sub} :params} (str "b|" pid "|" sub)))]
    (testing "matching actions"
      (is (= {:status :ok
              :output ["a|cheese"]}
             (x {:action "/product/cheese/add"})))
      (is (= {:status :ok
              :output ["b|cheese|mild"]}
             (x {:action "/product/cheese/push/mild"}))))))
