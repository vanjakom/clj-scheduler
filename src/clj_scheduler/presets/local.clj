(ns clj-scheduler.presets.local
  (:require
   [clj-common.as :as as]
   [clj-common.path :as path]
   [clj-scheduler.core :as core]
   [clj-scheduler.job.osm :as osm]
   [clj-scheduler.job.pss :as pss]
   [clj-scheduler.job.system :as system]
   [clj-scheduler.trigger :as trigger]))

(def dataset-local-path ["Users" "vanja" "dataset-local"])

(def geofabrik-serbia-path (path/child dataset-local-path "geofabrik-serbia"))
(def geofabrik-serbia-latest-path
  (path/child geofabrik-serbia-path "serbia-latest.osm.pbf"))

;; general

(trigger/interval
 (* 24 60 60 1000)
 "job-cleanup"
 "job-cleanup"
 {
  :keep-last 10}
 system/job-cleanup)

;; serbia

;; switch to manual trigger
#_(trigger/interval
 (* 24 60 60 1000)
 "geofabrik-serbia-download"
 "geofabrik-serbia-download"
 {
  :state-done-node ["geofabrik" "serbia" "download"]
  :store-path geofabrik-serbia-path}
 osm/download-latest-geofabrik-serbia)
(trigger/manual-trigger
 "geofabrik-serbia-download"
 {
  :state-done-node ["geofabrik" "serbia" "download"]
  :store-path geofabrik-serbia-path}
 osm/download-latest-geofabrik-serbia)

(trigger/interval
 (* 24 60 60 1000)
 "geofabrik-serbia-cleanup"
 "geofabrik-serbia-cleanup"
 (let [match-fn (fn [file]
                  (let [name (last file)]
                    (when (.startsWith name "serbia")
                      (let [timestamp (->
                                  name
                                  (.replace "serbia-" "")
                                  (.replace ".osm.pbf" ""))]
                        (when-not (= timestamp "latest")
                          (as/as-long timestamp))))))]
   {
    :directory geofabrik-serbia-path
    :match-fn match-fn
    :delete-fn (fn [file-seq]
                 (drop 3 (reverse (sort-by match-fn file-seq))))
    :dry-run false})
 system/watch-directory)

(core/job-sumbit
 (core/job-create
  "startup-ensure-geofabrik-serbia-split-directory"
  {
   :directory (path/child dataset-local-path "geofabrik-serbia-split")}
  system/ensure-directory))

(trigger/on-state-change
 ["geofabrik" "serbia" "download"]
 "geofabrik-serbia-split"
 {
  :osm-pbf-path geofabrik-serbia-latest-path
  :osm-node-path (path/child dataset-local-path "geofabrik-serbia-split" "node.edn")
  :osm-node-with-tags-path (path/child dataset-local-path "geofabrik-serbia-split" "node-with-tags.edn")
  :osm-way-path (path/child dataset-local-path "geofabrik-serbia-split" "way.edn")
  :osm-relation-path (path/child dataset-local-path "geofabrik-serbia-split" "relation.edn")
  :state-done-node ["geofabrik" "serbia" "split"]}
 osm/split-geofabrik-pbf-nwr)

(trigger/manual-trigger
 "tile-download-osm-serbia-z10"
 {
   
  :store-path ["Users" "vanja" "dataset-local" "tile-osm-serbia-z10"]
  :tile-server-url "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
  :state-done-node ["tile-download" "serbia-z10"]
  :upper-left-tile [10 565 363]
  :lower-right-tile [10 578 381]}
 osm/download-tile-bounds)

(trigger/on-state-change
 ["tile-download" "serbia-z10"]
 "tile-image-osm-serbia-z10"
 {
  :store-path ["Users" "vanja" "dataset-local" "tile-image" "osm-serbia-z10.png"]
  :tile-path ["Users" "vanja" "dataset-local" "tile-osm-serbia-z10"]
  :state-done-node ["tile-image" "serbia-z10"]
  :upper-left-tile [10 565 363]
  :lower-right-tile [10 578 381]}
 osm/image-from-tiles)

;; pss jobs

(core/job-sumbit
 (core/job-create
  "startup-ensure-osm-pss-extract-directory"
  {
   :directory (path/child dataset-local-path "osm-pss-extract")}
  system/ensure-directory))

(trigger/manual-trigger
 "pss-website-dataset"
 {
  :pss-dataset-path ["Users" "vanja" "dataset-git" "pss.rs"]
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]}
 pss/prepare-pss-dataset)

;; relation mapping could be disabled after all routes are mapped and manually updated
;; switch pss-extract to depend on geofabrik-serbia-split
#_(trigger/on-state-change
 ["geofabrik" "serbia" "split"]
 "pss-relation-mapping"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :state-done-node ["pss" "relation-mapping"]}
 pss/extract-pss-ref-osm-relation-id-mapping)
;; 20231212 switch to manual trigger since Fruskogorska transverzala is not labeled
(trigger/manual-trigger
 "pss-relation-mapping"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :state-done-node ["pss" "relation-mapping"]}
 pss/extract-pss-ref-osm-relation-id-mapping)

;; pss extract is not depending on relation mapping trigger since it will be disabled
(trigger/on-state-change
 #_["pss" "relation-mapping"]
 ["geofabrik" "serbia" "split"]
 "pss-extract"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :state-done-node ["pss" "extract"]}
 pss/extract-pss-osm)

(trigger/on-state-change
 ["pss" "extract"]
 "pss-stats"
 {
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :state-done-node ["pss" "stats"]}
 pss/extract-pss-stats)

(trigger/on-state-change
 ["pss" "extract"]
 "pss-geojson-combined-map"
 {
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :state-done-node ["pss" "geojson-combined-map"]}
 pss/extract-geojson-combined-map)

(trigger/on-state-change
 ["pss" "extract"]
 "pss-geojson-per-trail-map"
 {
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :state-done-node ["pss" "pss-geojson-per-trail-map"]}
 pss/extract-geojson-per-trail)

(trigger/on-state-change
 ["pss" "extract"]
 "pss-geojson-with-trails"
 {
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset"]
  :state-done-node ["pss" "pss-geojson-with-trails"]}
 pss/extract-geojson-with-trails)


(trigger/on-state-change
 ["pss" "extract"]
 "pss-trails-image-osm-red"
 {
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :background-image-path ["Users" "vanja" "dataset-local" "tile-image" "osm-serbia-z10.png"]
  :image-path ["Users" "vanja" "projects" "osm-pss-integration" "dataset" "trails-osm-z10-red.png"]
  :state-done-node ["pss" "pss-geojson-per-trail-map"]
  :upper-left-tile [10 565 363]
  :lower-right-tile [10 578 381]}
 pss/create-trails-image)


;; git checks
(trigger/manual-trigger
 "git-check-projects"
 {
  :repo-root ["Users" "vanja" "projects"]
  :state-root ["git" "projects"]}
 system/git-status-repo-root)

