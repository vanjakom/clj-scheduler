(ns clj-scheduler.job.osm
  (:require
   [clj-common.context :as context]
   [clj-common.http :as http]
   [clj-common.io :as io]
   [clj-common.localfs :as fs]
   [clj-common.path :as path]
   [clj-common.pipeline :as pipeline]
   [clj-geo.import.osm :as import]
   [clj-scheduler.core :as core]))

;; legacy from old trek-mate times
(def active-pipeline nil)

(defn download-latest-geofabrik-serbia [context]
  (let [state-done-node (get (core/context-configuration context) :state-done-node)
        store-path (get (core/context-configuration context) :store-path)
        upstream-url "http://download.geofabrik.de/europe/serbia-latest.osm.pbf"
        date-extract-url "http://download.geofabrik.de/europe/serbia.html"
        timestamp (System/currentTimeMillis)
        download-path (path/child store-path (str "serbia-" timestamp ".osm.pbf"))
        latest-path (path/child store-path "serbia-latest.osm.pbf")]
    (when (not (fs/exists? store-path))
      (fs/mkdirs store-path)
      (core/context-report
       context
       (str "store path created: " (path/path->string store-path))))
    ;; todo use date from html page for timestamp
    (core/context-report context "downloading latest Geofabrik OSM dump for Serbia")
    (with-open [is (http/get-as-stream upstream-url)
                os (fs/output-stream download-path)]
      (io/copy-input-to-output-stream is os))
    (core/context-report context "latest Serbia OSM dump downloaded")
    (when (fs/exists? latest-path)
      (fs/delete latest-path))
    (fs/link download-path latest-path)
    (core/context-report context "latest Serbia OSM dump linked as latest")

    ;; todo add trigger
    (core/state-set state-done-node timestamp)
    (core/context-report context (str "state set at " state-done-node))))

(defn split-geofabrik-pbf-nwr [job-context]
  (let [context (core/context-pipeline-adapter job-context)
        channel-provider (pipeline/create-channels-provider)
        resource-controller (pipeline/create-trace-resource-controller context)
        osm-pbf-path (get
                      (core/context-configuration job-context)
                      :osm-pbf-path)
        osm-node-path (get
                      (core/context-configuration job-context)
                      :osm-node-path)
        osm-node-with-tags-path (get
                      (core/context-configuration job-context)
                      :osm-node-with-tags-path)        
        osm-way-path (get
                      (core/context-configuration job-context)
                      :osm-way-path)
        osm-relation-path (get
                           (core/context-configuration job-context)
                           :osm-relation-path)
        state-done-node (get
                         (core/context-configuration job-context)
                         :state-done-node)
        timestamp (System/currentTimeMillis)]
    (import/read-osm-pbf-go
     (context/wrap-scope context "read")
     osm-pbf-path
     ;; use ram path
     ;;osm-pbf-ram-path
     (channel-provider :node-multiplex-in)
     (channel-provider :way-in)
     (channel-provider :relation-in))

    (pipeline/broadcast-go
     (context/wrap-scope context "node-multiplex")
     (channel-provider :node-multiplex-in)
     (channel-provider :node-with-tags-in)
     (channel-provider :node-in))

    (pipeline/transducer-stream-go
     (context/wrap-scope context "filter-node-with-tags")
     (channel-provider :node-with-tags-in)
     (filter #(not (empty? (:tags %))))
     (channel-provider :write-node-with-tags-in))
    (pipeline/write-edn-go
     (context/wrap-scope context "write-node-with-tags")
     resource-controller
     osm-node-with-tags-path
     (channel-provider :write-node-with-tags-in))
    
    (pipeline/write-edn-go
     (context/wrap-scope context "write-node")
     resource-controller
     osm-node-path
     (channel-provider :node-in))

    (pipeline/write-edn-go
     (context/wrap-scope context "write-way")
     resource-controller
     osm-way-path
     (channel-provider :way-in))

    (pipeline/write-edn-go
     (context/wrap-scope context "write-relation")
     resource-controller
     osm-relation-path
     (channel-provider :relation-in))
    (alter-var-root #'active-pipeline (constantly (channel-provider)))
    (core/wait-pipeline-job context)

    (core/state-set state-done-node timestamp)
    (core/context-report job-context (str "state set at " state-done-node))))
