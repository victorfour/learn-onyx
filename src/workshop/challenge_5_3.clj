(ns workshop.challenge-5-3
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :process-user]
   [:process-user :write-segments]])

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

      {:onyx/name :process-user
       :onyx/fn :workshop.challenge-5-3/process-user
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Returns the segment"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn process-user [segment]
  (if (= (:status segment) :guest)
    (throw (ex-info "User is a guest" {:type :guest :segment segment}))
    segment))

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

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-5-3/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;;; Flow conditions ;;;

;; <<< BEGIN FILL ME IN >>>

(defn post-transform
  [event segment ex]
  (assoc segment :error "Insufficient access level"))

(defn handle-error?
  [event old ex-obj all-new]
  (= (:type (ex-data ex-obj)) :guest))

(def flow-conditions
  [{:flow/from :process-user 
    :flow/to [:write-segments]
    :flow/short-circuit? true
    :flow/thrown-exception? true
    :flow/post-transform :workshop.challenge-5-3/post-transform
    :flow/predicate :workshop.challenge-5-3/handle-error?}])
  
;; <<< END FILL ME IN >>>
