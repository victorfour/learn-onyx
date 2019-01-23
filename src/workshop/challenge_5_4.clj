(ns workshop.challenge-5-4
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :children]
   [:identity :adults]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :identity
       :onyx/fn :clojure.core/identity
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Returns the segment"}

      {:onyx/name :children
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}

      {:onyx/name :adults
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Lifecycles ;;;

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :adults
    :lifecycle/calls :workshop.challenge-5-4/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :adults
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :children
    :lifecycle/calls :workshop.challenge-5-4/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :children
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;;; Flow conditions ;;;

;; <<< BEGIN FILL ME IN >>>
(defn adult?
  [event old-segment new-segment all-new-segments]
  (<= 18 (:age new-segment)))

(defn children?
  [event old-segment new-segment all-new-segments]
  (> 18 (:age new-segment)))


(def flow-conditions
  [{:flow/from :identity
    :flow/to [:adults]
    :flow/predicate :workshop.challenge-5-4/adult?
    :flow/exclude-keys [:age]
    :flow/doc "Route to :adults if :age in this segment is greater than 18"}
   
   {:flow/from :identity
    :flow/to [:children]
    :flow/predicate :workshop.challenge-5-4/children?
    :flow/exclude-keys [:age]
    :flow/doc "Route to :children if :age in this segment is less than 18"}])

;; <<< END FILL ME IN >>>
