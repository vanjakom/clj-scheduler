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

(trigger/interval
 (* 24 60 60 1000)
 "job-cleanup"
 "job-cleanup"
 {
  :keep-last 10}
 system/job-cleanup)

(trigger/interval
 (* 24 60 60 1000)
 "geofabrik-serbia-download"
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
 job/watch-directory)

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

(core/job-sumbit
 (core/job-create
  "startup-ensure-osm-pss-extract-directory"
  {
   :directory (path/child dataset-local-path "osm-pss-extract")}
  system/ensure-directory))

;; relation mapping could be disabled after all routes are mapped and manually updated
(trigger/on-state-change
 ["geofabrik" "serbia" "split"]
 "pss-relation-mapping"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :state-done-node ["pss" "relation-mapping"]}
 pss/extract-pss-ref-osm-relation-id-mapping)

;; pss extract is not depending on relation mapping trigger since it will be disabled
(trigger/on-state-change
 ["geofabrik" "serbia" "split"]
 "pss-extract"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :state-done-node ["pss" "extract"]}
 pss/extract-pss-osm)


#_(trigger/on-state-change
 ["pss" "extract"]
 "pss-stats"
 {
  :osm-pss-integration-path ["Users" "vanja" "projects" "osm-pss-integration"]
  :geofabrik-serbia-split-path ["Users" "vanja" "dataset-local" "geofabrik-serbia-split"]
  :osm-pss-extract-path ["Users" "vanja" "dataset-local" "osm-pss-extract"]
  :state-done-node ["pss" "extract"]}
 pss/extract-pss-osm)
