(ns clj-scheduler.trigger
  (:require
   [clj-scheduler.core :as core]))

(defn interval [interval name job-name job-configuration job-fn]
  (core/trigger-register
   name
   (fn [context]
     (let [last (or (core/state-get ["trigger" name "last"]) 0)
           current (System/currentTimeMillis)]
       (when (> (- current last) interval)
         (core/context-report context (str "trigger job at " current))
         (core/job-sumbit
          (core/job-create
           job-name job-configuration job-fn))
         (core/state-set ["trigger" name "last"] current))))
   {}))

(defn on-state-change [state-node name job-configuration job-fn]
  (core/trigger-register
   name
   (fn [context]
     (let [timestamp (System/currentTimeMillis)
           last-value (core/state-get ["trigger" name "last-trigger-on"])
           current-value (core/state-get state-node)]
       (when (or (nil? last-value)
                 (and
                  (some? current-value)
                  (not (= last-value current-value))))
         (core/context-report
          context
          (str "trigger on " current-value " previous: " last-value))
         (let [id (core/job-sumbit
                   (core/job-create name job-configuration job-fn))]
           (core/context-report context (str "submitted job: " id))
           (core/state-set ["trigger" name "last-trigger-on"] current-value)
           (core/state-set ["trigger" name "last"] timestamp)
           (core/state-set ["trigger" name "last-job-id"] id)))))
   {}))
