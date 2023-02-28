(ns clj-scheduler.main
  (:use
   clj-common.clojure)
  (:require
   [clj-common.http-server :as server]
   [hiccup.core :as hiccup]
   compojure.core
   [clj-scheduler.core :as core]
   [clj-scheduler.env :as env]
   [clj-scheduler.job :as job]
   [clj-scheduler.trigger :as trigger]))

;; todo support seq html representation

(defn html-map->table [dictionary]
  [:table {:style "border-collapse:collapse;"}
   (map
    (fn [[key value]]
      [:tr
       [:td {:style "border: 1px solid black; padding: 5px;"}
        key]
       [:td {:style "border: 1px solid black; padding: 5px;"}
        (if (map? value)
          (html-map->table value)
          value)]])
    dictionary)])

(server/create-server
 env/http-server-port
 (compojure.core/routes
  (compojure.core/GET
   "/"
   _
   {
    :status 200
    :body (hiccup/html
           [:body  {:style "font-family:arial;"}
            [:div "jobs:"]
            [:br]
            [:table {:style "border-collapse:collapse;"}
             (map
              (fn [job]
                (let [state (deref (:state job))]
                  [:tr
                   [:td {:style "border: 1px solid black; padding: 5px;"}
                    [:a {:href (str "/job/" (:id job)) :target "_blank"} (:id job)]]
                   [:td {:style "border: 1px solid black; padding: 5px;"}
                    (:name job)]
                   [:td {:style "border: 1px solid black; padding: 5px;"}
                    (:status state)]]))
              (deref core/jobs))]
            [:br]
            [:br]
            [:div "state:"]
            [:br]
            [:table {:style "border-collapse:collapse;"}
             (html-map->table (deref core/state))]
            [:br]
            [:br]
            [:div "triggers:"]
            [:br]
            [:table {:style "border-collapse:collapse;"}
             (map
              (fn [[name trigger]]
                [:tr
                 [:td {:style "border: 1px solid black; padding: 5px;"}
                  [:a {:href (str "/trigger/" (url-encode name))
                       :target "_blank"}
                   name]]])
              (deref core/triggers))]])})

  ;; todo expose state manupulation ( get, set ) over http
  
  (compojure.core/GET
   "/job/:id"
   [id]
   (if-let [job (first (filter #(= (:id %) id) (deref core/jobs)))]
     (let [state (deref (:state job))
           out (:out state)
           counters (:counters state)]
       {
        :status 200
        :body (hiccup/html
               [:body {:style "font-family:arial;"}
                [:div (str "job: " (:id job))]
                [:br]
                [:div "counters:"]
                (map (fn [[counter value]]
                       [:div (str counter " = " value)])
                     counters)
                [:br]
                [:div "output:"]
                (map (fn [line]
                       [:div line])
                     out)])})
     {
      :status 404
      :body "unknown job id"}))

  (compojure.core/GET
   "/trigger/:name"
   [name]
   (let [name (url-decode name)]
     (if-let [trigger (get (deref core/triggers) name)]
            (let [out (get-in trigger [:state :out])
                  counters (get-in trigger [:state :counters])]
              {
               :status 200
               :body (hiccup/html
                      [:body {:style "font-family:arial;"}
                       [:div (str "trigger: " name)]
                       [:br]
                       [:div "counters:"]
                       (map (fn [[counter value]]
                              [:div (str counter " = " value)])
                            counters)
                       [:br]
                       [:div "output (last 100):"]
                       (map (fn [line]
                              [:div line])
                            out)])})
            {
             :status 404
             :body "unknown trigger name"})))))

(defn -main [& args]
  (println "scheduler started")
  (when (not (empty? args))
    (let [ns-to-load (first args)]
      (println "loading:" ns-to-load)
      (load (.replace
             (.replace ns-to-load "." "/")
             "-" "_"))
      (println "loaded:" ns-to-load))))

#_(core/job-sumbit
 (core/job-create
  "hello world"
  {}
  job/hello-world))

#_(trigger/interval
 (* 30 1000)
 "hello-world"
 "hello world"
 {}
 job/hello-world)
#_(core/trigger-unregister "hello-world")
