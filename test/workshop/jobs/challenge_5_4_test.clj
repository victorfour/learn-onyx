(ns workshop.jobs.challenge-5-4-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-5-4 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; The final feature of flow conditions that we'll cover are
;; key exclusions. Sometimes, you'll want to convey some information
;; from a function to a flow condition predicate that doesn't
;; need to be sent downstream. In the example below, we're going
;; to route users to different tasks based on their age.
;; Their age is only relevant to the routing, and isn't required after that.
;;
;; Use :flow/exclude-keys to remove the :age key from all the segments
;; after it passes through the :identity function.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-5-4-test`
;;

(def input
  [{:username "Mike" :age 24}
   {:username "Linda" :age 35}
   {:username "Lucas" :age 32}
   {:username "Ron" :age 16}
   {:username "Kara" :age 14}])

(def expected-adult-output
  [{:username "Mike"}
   {:username "Linda"}
   {:username "Lucas"}])

(def expected-child-output
  [{:username "Ron"}
   {:username "Kara"}])

(deftest test-level-5-challenge-4
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[children adults] (u/collect-outputs! lifecycles [:children :adults])]
          (u/segments-equal? expected-child-output children)
          (u/segments-equal? expected-adult-output adults))))))
