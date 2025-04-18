(ns clj-scheduler.core
  (:use
   clj-common.clojure)
  (:require
   [clj-common.context :as context]
   [clj-common.edn :as edn]
   [clj-common.localfs :as fs]
   [clj-common.time :as time]
   [clj-scheduler.env :as env]))

;; concepts
;; state, key value exposed to triggers, jobs and over http, persistent, shared
;; trigger, called on fixed interval, based on state and behavior triggers job
;; job, something doing something, uses state if needed, must finish

(def state (atom
            (if (fs/exists? env/state-path)
              (with-open [is (fs/input-stream env/state-path)]
                (edn/read-object is))
              {})))

(defn state-get [keys]
  (get-in (deref state) keys))

(defn state-set [keys value]
  (swap! state update-in keys (constantly value))
  (with-open [os (fs/output-stream env/state-path)]
    (edn/write-object os (deref state)))
  nil)

(defn state-unset [keys]
  (if (> (count keys) 1)
    (swap! state update-in (drop-last keys) #(dissoc % (last keys)))
    (swap! state dissoc (first keys))))

#_(state-set ["test" "name"] "vanja")
#_(state-set ["test" "id"] "vanjakom")
#_(state-get ["test" "name"])

;; order of jobs ( vector or list ) is not important
;; all operations relay on fields to operate
(def jobs (atom '()))

(defn job-create-id []
  (str "job-" (System/currentTimeMillis)))

(defn job-create [name configuration job-fn]
  {
   :id (job-create-id)
   :name name
   :configuration configuration
   :queue "main"
   :fn job-fn
   :state (atom {
                 :status :waiting
                 :out []
                 :counters {}})})

(defn job-sumbit [job]
  (swap!
   jobs
   conj
   (assoc job :submitted-at (System/currentTimeMillis)))
  (:id job))

(defn job-remove [id]
  (swap!
   jobs
   #(reduce
     (fn [jobs job]
       (if (and
            (= (:id job) id)
            (not (= (:status (deref (:state job))) :running)))
         jobs
         (conj jobs job)))
     []
     %)))

(defn job-status [job]
  (:status (deref (:state job))))

;; api for retrieval / use of context

;; old version, deprecated, use clj-common.context

;; DEPRECATED, use context fns
(defn context-report [context message]
  (context/trace context message))

;; DEPRECATED, use context fns
(defn context-counter [context counter]
  (context/increment-counter context counter))

;; DEPRECATED, use context fns
(defn context-configuration [context]
  (context/configuration context))

;; DEPRECATED, use raw context
(defn context-pipeline-adapter
  "Creates context that could be used by pipelines to report to
  clj-scheduler context"
  [context]
  context
  #_(context/wrap-scope
   (let [counter-fn (fn [scope counter]
                      (context-counter
                       context
                       (str
                        (clojure.string/join "." scope)
                        "." counter)))
         state-map (atom {})]
     {
      :scope-counter-fn counter-fn
      :scope-state-fn (fn [scope state]
                        ;; concurrent update is not possible
                        (let [current (get (deref state-map) scope)]
                          (when (not (= state current))
                            (swap! state-map assoc scope state)
                            (context-report
                             context
                             (str
                              "[STATE] "
                              (clojure.string/join "." scope)
                              " from " current " to " state) ))))
      :scope-trace-fn (fn [scope trace]
                        (context-report context (str "["
                                                     (clojure.string/join "." scope)
                                                     "] "
                                                     trace)))
      :scope-error-fn (fn [scope throwable data]
                        (let [output (str
                                      (throwable->string throwable)
                                      (if-let [data data]
                                        (str "Data:\n" data)
                                        "No data"))]
                          (context-report context (str "["
                                                       (clojure.string/join "." scope)
                                                       "] "
                                                       output))
                          (counter-fn (clojure.string/join "." scope) "exception")))
      :context-dump-fn (fn [] (throw (new Exception "Not implemented")))
      :context-print-fn (fn [] (throw (new Exception "Not implemented")))

      :pipeline-complete-fn (fn []
                              (let [state-map (deref state-map)]
                                (and
                                 ;; hack to solve call to wait before first go
                                 ;; todo solve better
                                 (> (count state-map) 0)
                                 (not
                                  (some?
                                   (first
                                    (filter
                                     #(not (= % "completion"))
                                     (vals state-map))))))))})))

;; todo
;; finish implementation
;; not using scope, should?

(defn create-context [job]
  (let [trace-fn (fn [scope trace]
                   (swap!
                    (:state job)
                    update-in [:out] conj trace)
                   (println trace))
        counter-fn (fn [scope counter]
                     (swap!
                      (:state job)
                      update-in
                      [:counters
                       (str
                        (clojure.string/join "." scope)
                        "." counter)]
                      #(inc (or % 0))))
        ;; support for pipeline integration
        state-map (atom {})]
    (context/wrap-scope
     {
      :scope-counter-fn counter-fn
      :scope-trace-fn trace-fn
      :scope-error-fn
      (fn [scope throwable data]
        (let [output (str
                      (throwable->string throwable)
                      (if-let [data data]
                        (str "Data:\n" data)
                        "No data"))]
          (trace-fn scope output)
          (counter-fn (clojure.string/join "." scope) "exception"))
        nil)

      :context-dump-fn (fn [] (throw (new Exception "Not implemented")))
      :context-print-fn (fn [] (throw (new Exception "Not implemented")))
      
      :store-get state-get

      :store-set state-set

      ;; additional info in context, should add to main one?
      :id (:id job)
      :name (:name job)
      :configuration (:configuration job)

      ;; support for pipeline integration
      :scope-state-fn (fn [scope state]
                        ;; concurrent update is not possible
                        (let [current (get (deref state-map) scope)]
                          (when (not (= state current))
                            (swap! state-map assoc scope state)
                            (trace-fn
                             scope
                             (str
                              "[STATE] " scope " from " current " to " state)))))
      :pipeline-complete-fn (fn []
                              (let [state-map (deref state-map)]
                                (and
                                 ;; hack to solve call to wait before first go
                                 ;; todo solve better
                                 (> (count state-map) 0)
                                 (not
                                  (some?
                                   (first
                                    (filter
                                     #(not (= % "completion"))
                                     (vals state-map))))))))})))

(defn wait-pipeline-job
  "Waits all actors finish ( move to completion state )"
  [pipeline-context]
  ((:scope-trace-fn  pipeline-context) ["pipeline"] "waiting pipeline to finish")
  (while (not ((:pipeline-complete-fn pipeline-context)))
    (sleep 1000))
  ((:scope-trace-fn  pipeline-context) ["pipeline"] "pipeline finished"))

(defn jobs-drop []
  (swap! jobs (constantly '())))

#_(jobs-drop)

(defn jobs-cleanup [keep-last-finished]
  (swap! jobs
         (fn [jobs]
           (first
            (reduce
             (fn [[jobs left] next]
               (if (not (= :finished (job-status next)))
                 [(conj jobs next) left]
                 (if (> left 0)
                   [(conj jobs next) (dec left)]
                   [jobs left])))
             ['() keep-last-finished]
             jobs)))))

#_(jobs-cleanup 20)

;; todo single worker assumed, not thread safe
(def worker-thread-main
  (new
   Thread
   (fn []
     (.setName (Thread/currentThread) "main-worker")
     (println "[main-worker] starting")
     (try
       (while true
        (if-let [job (first (sort-by
                             :submitted-at
                             (filter
                              #(= (:status (deref (:state %))) :waiting)
                              (deref jobs))))]
          (do
            (println "[main-worker] running job: " (:id job) )
            (swap! (:state job) assoc :status :running)
            (let [context (create-context job)]
              (try
                (context/trace context "starting job")
                ((:fn job) context)
                (swap! (:state job) assoc :status :finished)
                ;; 20250327 state done node should be populated from worker
                ;; no need to replicate same code in each job
                (when-let [state-done-node (get
                                            (context/configuration context)
                                            :state-done-node)]
                  (context/store-set context  state-done-node (System/currentTimeMillis))
                  (context/trace context (str "state set at " state-done-node)))
                (catch Exception e
                  ;; todo capture exception to log
                  (println "exception in job" (:id job))
                  (println (format-stack-trace e))
                  (context/trace context "exception in job")
                  (context/trace context (format-stack-trace e))
                  (swap! (:state job) assoc :status :failed)))))
          (do
            (state-set ["system" "worker" "main" "last"] (System/currentTimeMillis))
            (java.lang.Thread/sleep 5000))))
       (catch Exception e
         (println "[main-worker] exiting")
         (.printStackTrace e))))))
(.start worker-thread-main)
#_(.interrupt worker-thread-main)

(def triggers (atom {}))

(defn trigger-register [name trigger-fn configuration]
  (swap! triggers assoc name {
                              :state {:counters {} :out []}
                              :trigger-fn trigger-fn
                              :configuration configuration}))

(defn trigger-unregister [name]
  (swap! triggers dissoc name))

(defn triggers-clear []
  (swap! triggers (constantly {})))

(triggers-clear)

(def trigger-thread
  (new
   Thread
   (fn []
     (.setName (Thread/currentThread) "trigger-thread")
     (println "[trigger] starting")
     (while true
       (doseq [[name trigger] (deref triggers)]
         ;; must be string because it's altered over ui
         (when (not (= "true" (state-get ["system" "trigger" "pause"])))
           (let [context (context/wrap-scope
                          {
                           :scope-counter-fn
                           (fn [scope counter]
                             ;; todo ignoring scope
                             (swap!
                              triggers
                              update-in
                              [name :state :counters counter]
                              #(inc (or % 0)))
                             nil)
                           :scope-trace-fn
                           (fn [scope trace]
                             ;; todo ignoring scope
                             (swap!
                              triggers
                              update-in [name :state :out]
                              #(take-last 100 (conj % trace)))
                             (println trace))
                           
                           :scope-state-fn
                           (fn [scope state]
                             nil)
                           :scope-error-fn
                           (fn [scope throwable data]
                             nil)

                           :store-get
                           (fn [keys] nil)

                           :store-set
                           (fn [keys] nil)

                           ;; additional info in context, should add to main one?
                           :id (str name "-" (time/timestamp))
                           :name name
                           :configuration (:configuration trigger)})]
             (try
               ((:trigger-fn trigger) context)
               (catch Exception e
                 (.printStackTrace e)
                 (println (str "[trigger] failed " name)))))))
       (state-set ["system" "trigger" "last"] (System/currentTimeMillis))
       (java.lang.Thread/sleep 5000)))))

(.start trigger-thread)
#_(.interrupt trigger-thread)

#_(state-set ["system" "trigger" "pause"] "true")
