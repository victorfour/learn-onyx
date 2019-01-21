(ns workshop.jobs.challenge-3-1-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-3-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Now that you know the rules of functions, let's implement some
;; functions for a workflow. This Onyx job will have 3 function tasks.
;; You'll need to implement the functions to pass the test.  Use a
;; generic function that builds on the lessons from challenge_2_2.
;; Read the docstrings on the corresponding catalog entries for the
;; functions' purposes.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-1-test`
;;

(def input
  [{:name "Mike"}
   {:name "Lucas"}
   {:name "Tim"}
   {:name "Aaron"}
   {:name "Lauren"}
   {:name "Bob"}
   {:name "Fred"}
   {:name "Lisa"}
   {:name "Tina"}])

(def expected-output
  [{:name "M | I | K | E"}
   {:name "L | U | C | A | S"}
   {:name "T | I | M"}
   {:name "A | A | R | O | N"}
   {:name "L | A | U | R | E | N"}
   {:name "B | O | B"}
   {:name "F | R | E | D"}
   {:name "L | I | S | A"}
   {:name "T | I | N | A"}])

(deftest test-level-3-challenge-1
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
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
