(ns clj-scheduler.presets.dev1
  (:require
   [clj-common.as :as as]
   [clj-common.path :as path]
   [clj-scheduler.job.osm :as osm]
   [clj-scheduler.job.system :as system]
   [clj-scheduler.trigger :as trigger]))

(trigger/interval
 (* 24 60 60 1000)
 "cleanup-osm-notes-data"
 "cleanup-osm-notes-data"
 (let [match-fn (fn [file]
                  (let [name (last file)]
                    (when (.startsWith name "planet-notes")
                      (let [date (->
                                  name
                                  (.replace "planet-notes-" "")
                                  (.replace ".osn.bz2" ""))]
                        (when-not (= date "latest")
                          date)))))]
   {
    :directory (path/string->path "/home/ec2-user/dataset/osm-planet-notes/")
    :match-fn match-fn
    :delete-fn (fn [file-seq]
                 (drop 3 (reverse (sort-by match-fn file-seq))))
    :dry-run false})
 system/watch-directory)

#_(trigger/interval
 (* 24 60 60 1000)
 "download-geofabrik-serbia"
 "download-geofabrik-serbia"
 {
  :state-done-node ["download" "geofabrik" "serbia"]
  :store-path ["home" "ec2-user" "dataset" "geofabrik-serbia"]}
 osm/download-latest-geofabrik-serbia)

#_(trigger/interval
 (* 24 60 60 1000)
 "cleanup-geofabrik-serbia"
 "cleanup-geofabrik-serbia"
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
    :directory (path/string->path "/home/ec2-user/dataset/geofabrik-serbia/")
    :match-fn match-fn
    :delete-fn (fn [file-seq]
                 (drop 3 (reverse (sort-by match-fn file-seq))))
    :dry-run false})
 job/watch-directory)


#_(trigger/on-state-change
 ["download" "geofabrik" "serbia"]
 "split geofabrik-serbia"
 {
  :osm-pbf-path ["home" "ec2-user" "dataset" "geofabrik-serbia" "serbia-latest.osm.pbf"]
  :osm-node-path ["home" "ec2-user" "dataset" "geofabrik-serbia-split" "node.edn"]
  :osm-node-with-tags-path ["home" "ec2-user" "dataset" "geofabrik-serbia-split" "node-with-tags.edn"]
  :osm-way-path ["home" "ec2-user" "dataset" "geofabrik-serbia-split" "way.edn"]
  :osm-relation-path ["home" "ec2-user" "dataset" "geofabrik-serbia-split" "relation.edn"]}
 osm/split-geofabrik-pbf-nwr)
