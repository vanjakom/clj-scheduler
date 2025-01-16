(ns clj-scheduler.job.osm
  (:use
   clj-common.clojure)
  (:require
   [clj-http.client :as http-client]
   [clojure.java.io :as clojure-io]
   [clj-common.2d :as draw]
   [clj-common.context :as context]
   [clj-common.io :as io]
   [clj-common.http :as http]
   [clj-common.localfs :as fs]
   [clj-common.path :as path]
   [clj-common.pipeline :as pipeline]
   [clj-geo.import.osm :as import]
   [clj-scheduler.core :as core]))

;; legacy from old trek-mate times
(def active-pipeline nil)

;; in case of problems with download use
;; cd /Users/vanja/dataset-local/geofabrik-serbia
;; wget -O "serbia-$(date +%s)000.osm.pbf" "http://download.geofabrik.de/europe/serbia-latest.osm.pbf" && ln -sf "$(ls -t serbia-*.osm.pbf | head -n 1)" serbia-latest.osm.pbf
;; then manual trigger split process

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
    ;; 20250104
    ;; looks like problem is "sleep" on new mac
    ;; replacing clj-common.http and clj-common.io with direct clj-http and
    ;; clojure libs ( as suggested by chat gpt )
    (with-open [is (:body (http-client/get upstream-url {:as :stream}))
                os (clojure-io/output-stream (path/path->string download-path))]
      (clojure-io/copy is os))
    (core/context-report context "latest Serbia OSM dump downloaded")
    (when (fs/exists? latest-path)
      (fs/delete latest-path))
    (fs/link download-path latest-path)
    (core/context-report context "latest Serbia OSM dump linked as latest")

    ;; todo add trigger
    (core/state-set state-done-node timestamp)
    (core/context-report context (str "state set at " state-done-node))))

;; testing just download
#_(let [upstream-url "http://download.geofabrik.de/europe/serbia-latest.osm.pbf"
        store-path ["Users" "vanja" "dataset-local" "geofabrik-serbia"]
        timestamp (System/currentTimeMillis)
        download-path (path/child store-path (str "serbia-" timestamp ".osm.pbf"))]
    (with-open [is (http/get-as-stream upstream-url)
                os (fs/output-stream download-path)]
      (println "starting download")
      (io/copy-input-to-output-stream is os)
      (println "finished download")))

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

(defn download-tile-bounds
  "Downloads all tiles in given bounds from provided tile server"
  [context]
  (let [state-done-node (get (core/context-configuration context) :state-done-node)
        store-path (get (core/context-configuration context) :store-path)
        tile-server-url (get (core/context-configuration context) :tile-server-url)
        timestamp (System/currentTimeMillis)
        [zoom min-x min-y] (get (core/context-configuration context) :upper-left-tile)
        [zoom max-x max-y] (get (core/context-configuration context) :lower-right-tile)]
    (doseq [x (range min-x (inc max-x))]
      (doseq [y (range min-y (inc max-y))]
        (let [url (->
                   tile-server-url
                   (.replace "{z}" (str zoom))
                   (.replace "{x}" (str x))
                   (.replace "{y}" (str y)))
              path (path/child store-path (str zoom "_" x "_" y ".png"))]
          (core/context-report context (str "downloading: " url))
          (let [is (http/get-as-stream
                    url
                    {
                     :headers {"User-Agent" "trek-mate"}})]
            (with-open [os (fs/output-stream path)]
              (io/copy-input-to-output-stream is os)))
          (sleep 500))))
    (core/state-set state-done-node timestamp)
    (core/context-report context (str "state set at " state-done-node))))

(defn image-from-tiles
  "Use tiles downloaded with download-tile-bounds to create image"
  [context]
  (let [state-done-node (get (core/context-configuration context) :state-done-node)
        tile-path (get (core/context-configuration context) :tile-path)
        store-path (get (core/context-configuration context) :store-path)
        timestamp (System/currentTimeMillis)
        [zoom min-x min-y] (get (core/context-configuration context) :upper-left-tile)
        [zoom max-x max-y] (get (core/context-configuration context) :lower-right-tile)
        image-width (* (- max-x min-x) 256)
        image-height (* (- max-y min-y) 256)]
    (let [image (draw/create-image-context image-width image-height)]
      (doseq [x (range min-x (inc max-x))]
        (doseq [y (range min-y (inc max-y))]
          (let [path (path/child tile-path (str zoom "_" x "_" y ".png"))]
            (core/context-report context (str "processing [" zoom " " x " " y "]"))
            (with-open [is (fs/input-stream path)]
              (let [tile (draw/input-stream->image-context is)]
                (draw/draw-image
                 image
                 [(+ (* (- x min-x) 256) 128) (+ (* (- y min-y) 256) 128)]
                 tile))))))
      (with-open [os (fs/output-stream store-path)]
        (draw/write-png-to-stream image os)))
    
    (core/state-set state-done-node timestamp)
    (core/context-report context (str "state set at " state-done-node))))

#_(core/job-sumbit
 (core/job-create
  "download-osm-tile-pss"
  {
   
   :store-path ["Users" "vanja" "dataset-local" "tile-osm-pss"]
   :tile-server-url "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
   :state-done-node ["osm-pss" "tile-download"]
   :upper-left-tile [10 565 363]
   :lower-right-tile [10 578 381]}
  ;; todo
  download-tile-bounds))

#_(core/job-sumbit
 (core/job-create
  "background-osm-tile-pss"
  {
   :store-path ["Users" "vanja" "dataset-local" "tile-osm-pss" "image.png"]
   :tile-path ["Users" "vanja" "dataset-local" "tile-osm-pss"]
   :state-done-node ["osm-pss" "background-create"]
   :upper-left-tile [10 565 363]
   :lower-right-tile [10 578 381]}
  ;; todo
  image-from-tiles))


