(ns clj-scheduler.core
  (:require
   [clj-scheduler.env :as env]))

;; concepts
;; state, key value exposed to triggers, jobs and over http, persistent, shared
;; trigger, called on fixed interval, based on state and behavior triggers job
;; job, something doing something, uses state if needed, must finish

(def state (atom {}))

(defn state-get [& keys]
  (get-in (deref state) keys))

(defn state-set [value & keys]
  (swap! state update-in keys (constantly value)))

;; todo provide atomic get and set

#_(state-set "vanja" "test" "name")
#_(state-set "vanjakom" "test" "id")
#_(state-get "test" "name")

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
  (swap! jobs conj job))

;; api for retrieval / use of context

(defn context-report [context message]
  ((:report-fn context) message))

(defn context-counter [context counter]
  ((:inc-counter-fn context) counter))

(defn context-configuration [context]
  (:configuration context))

(defn jobs-drop []
  (swap! jobs (constantly '())))

#_(jobs-drop)

(defn jobs-cleanup []
  (swap! jobs take 10))

(def worker-thread-main
  (new
   Thread
   (fn []
     (println "[main-worker] starting")
     (while true
       (if-let [job (last (filter
                           #(= (:status (deref (:state %))) :waiting)
                           (deref jobs)))]
         (do
           (println "[main-worker] running job: " (:id job) )
           (swap! (:state job) assoc :status :running)
           (let [context {
                          :id (:id job)
                          :name (:name job)
                          :configuration (:configuration job)
                          :report-fn (fn [line]
                                       (swap!
                                        (:state job)
                                        update-in
                                        [:out]
                                        conj
                                        line)
                                       (println line))
                          :inc-counter-fn (fn [counter]
                                            (swap!
                                             (:state job)
                                             update-in
                                             [:counters counter]
                                             #(inc (or % 0))))}]
             (try
               ((:fn job) context)
               (swap! (:state job) assoc :status :finished)
               (catch Exception e
                 ;; todo capture exception to log
                 (.printStackTrace e)
                 (swap! (:state job) assoc :status :failed)))))
         (do
           (println "[main-worker] no job, sleeping")
           (java.lang.Thread/sleep 5000)))))))
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

(def trigger-thread
  (new
   Thread
   (fn []
     (println "[trigger] starting")
     (while true
       (doseq [[name trigger] (deref triggers)]
         (let [context {
                        :name name
                        :configuration (:configuration trigger)
                        :report-fn (fn [line]
                                     (swap!
                                      triggers
                                      update-in [name :state :out]
                                      #(take-last 100 (conj % line))))
                        :inc-counter-fn (fn [counter]
                                          (swap!
                                           triggers
                                           update-in
                                           [name :state :counters counter]
                                           #(inc (or % 0))))}]
           (try
             ((:trigger-fn trigger) context)
             (catch Exception e
               (.printStackTrace e)
               (println (str "[trigger] failed " name))))))
       (java.lang.Thread/sleep 5000)))))

(.start trigger-thread)
#_(.interrupt trigger-thread)
