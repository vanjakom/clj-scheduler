(ns clj-scheduler.trigger
  (:require
   [clj-scheduler.core :as core]))

(defn interval [interval name job-name job-configuration job-fn]
  (core/trigger-register
   name
   (fn [context]
     (let [last (or (core/state-get name "last") 0)
           current (System/currentTimeMillis)]
       (when (> (- current last) interval)
         (core/context-report context (str "trigger job at " current))
         (core/job-sumbit
          (core/job-create
           job-name job-configuration job-fn))
         (core/state-set current name "last" ))))
   {}))
