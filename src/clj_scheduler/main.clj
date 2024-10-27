(ns clj-scheduler.main
  (:use
   clj-common.clojure)
  (:require
   [clj-common.http-server :as server]
   [hiccup.core :as hiccup]
   compojure.core
   ring.util.response
   [clj-scheduler.core :as core]
   [clj-scheduler.env :as env]
   [clj-scheduler.trigger :as trigger]))

;; todo support seq html representation

(defn html-state->table [path dictionary]
  [:table {:style "border-collapse:collapse;"}
   (map
    (fn [[key value]]
      [:tr
       [:td {:style "border: 1px solid black; padding: 5px;"}
        key]
       [:td {:style "border: 1px solid black; padding: 5px;"}
        (if (map? value)
          (html-state->table (conj path key) value)
          [:div
           value
           [:a {:href (str "/state/unset/"
                           (clojure.string/join "/" (conj path key)))}
            "(unset)"]])]])
    dictionary)])

(defn html-configuration->table [dictionary]
  [:table {:style "border-collapse:collapse;"}
   (map
    (fn [[key value]]
      [:tr
       [:td {:style "border: 1px solid black; padding: 5px;"}
        key]
       [:td {:style "border: 1px solid black; padding: 5px;"}
        (cond
          (map? value)
          (html-configuration->table value)

          (vector? value)
          [:div (clojure.string/join "," value)]
          
          (seq? value)
          [:div (clojure.string/join "," value)]
          
          :else
          [:div value])]])
    dictionary)])

(defn start-server []
  (server/create-server
   env/http-server-port
   (compojure.core/routes
    (compojure.core/GET
     "/"
     _
     {
      :status 200
      :headers {
                "Content-Type" "text/html; charset=utf-8"}
      :body (hiccup/html
             [:body  {:style "font-family:arial;"}
              (let [timestamp (System/currentTimeMillis)
                    scheduler-timestamp (core/state-get ["system" "trigger" "last"])
                    worker-timestamp (core/state-get ["system" "worker" "main" "last"])]
                (if (< (- timestamp scheduler-timestamp) (* 60 1000))
                  (if (= "true" (core/state-get ["system" "trigger" "pause"]))
                    [:div
                     "scheduler is paused"
                     [:a {:href "/state/set/system/trigger/pause/false"} "(continue trigger)"]]
                    [:div
                     "scheduler is active"
                     [:a {:href "/state/set/system/trigger/pause/true"} "(pause trigger)"]])
                  [:div "scheduler is not active"]))
              [:br]
              [:div "jobs:"]
              [:br]
              [:table {:style "border-collapse:collapse;"}
               (map
                (fn [job]
                  (let [state (deref (:state job))
                        status (:status state)]
                    [:tr
                     [:td {:style "border: 1px solid black; padding: 5px;"}
                      [:a {:href (str "/job/" (:id job)) :target "_blank"} (:id job)]]
                     [:td {:style "border: 1px solid black; padding: 5px;"} (:name job)]
                     [:td {:style "border: 1px solid black; padding: 5px;"} status]
                     [:td {:style "border: 1px solid black; padding: 5px;"}
                      (when (not (= status :running))
                        [:a {:href (str "/job/" (:id job) "/remove") :target "_blank"}
                         "remove"])]
                     ]))
                (reverse
                 (sort-by
                  :submitted-at
                  (deref core/jobs))))]
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
                     name]]
                   [:td {:style "border: 1px solid black; padding: 5px;"}
                    [:a {:href (str "/trigger/" (url-encode name) "/manual")
                         :target "_blank"}
                     "manual trigger"]]])
                (sort-by first (deref core/triggers)))]
              [:br]
              [:br]
              [:div "state:"]
              [:br]
              [:table {:style "border-collapse:collapse;"}
               (html-state->table [] (deref core/state))]
              [:br]])})

    ;; todo expose state manupulation ( get, set ) over http

    (compojure.core/GET
     "/job/:id/remove"
     [id]
     (core/job-remove id)
     (ring.util.response/redirect "/"))
    
    (compojure.core/GET
     "/job/:id"
     [id]
     (if-let [job (first (filter #(= (:id %) id) (deref core/jobs)))]
       (let [state (deref (:state job))
             out (:out state)
             counters (:counters state)]
         {
          :status 200
          :headers {
                    "Content-Type" "text/html; charset=utf-8"}
          :body (hiccup/html
                 [:body {:style "font-family:arial;"}
                  [:div (str "job: " (:id job))]
                  [:br]
                  [:div "configuration:"]
                  (html-configuration->table (:configuration job))
                  [:br]
                  [:div "counters:"]
                  (map (fn [[counter value]]
                         [:div (str counter " = " value)])
                       (sort-by first counters))
                  [:br]
                  [:div "output:"]
                  (map (fn [line]
                         [:div line])
                       out)])})
       {
        :status 404
        :body "unknown job id"}))

    (compojure.core/GET
     "/trigger/:name/manual"
     [name]
     (core/state-set ["trigger" name "should-trigger"] true)
     (ring.util.response/redirect "/"))

    (compojure.core/GET
     "/trigger/:name"
     [name]
     (let [name (url-decode name)]
       (if-let [trigger (get (deref core/triggers) name)]
         (let [out (get-in trigger [:state :out])
               counters (get-in trigger [:state :counters])]
           {
            :status 200
            :headers {
                      "Content-Type" "text/html; charset=utf-8"}
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
          :body "unknown trigger name"})))

    (compojure.core/GET
     "/state/set/*"
     _
     (fn [request]
       (let [route (get-in request [:params :*])
             node-and-value (.split route "/")
             node (drop-last node-and-value)
             value (last node-and-value)]
         (core/state-set node value)
         (ring.util.response/redirect "/"))))

    (compojure.core/GET
     "/state/unset/*"
     _
     (fn [request]
       (let [route (get-in request [:params :*])
             node (.split route "/")]
         (core/state-unset node)
         (ring.util.response/redirect "/")))))))


(defn -main [& args]
  (start-server)
  (println "scheduler started")
  (println (str "http server running on: http://localhost:" env/http-server-port))
  (when (not (empty? args))
    (let [ns-to-load (first args)]
      (println "loading:" ns-to-load)
      ;; per load documentation path must begin with / to take it as full path
      (load (str "/" (.replace (.replace ns-to-load "." "/") "-" "_")))
      (println "loaded:" ns-to-load))))

;; auto load local presets
;; remove in deployment
;; no need for this when managed with uberjar
#_(-main "clj-scheduler.presets.local")

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

;; view at http://localhost:7076/ 
