(ns clj-scheduler.presets.dev1
  (:require
   [clj-common.path :as path]
   [clj-scheduler.job :as job]
   [clj-scheduler.trigger :as trigger]))

(trigger/interval
 (* 60 60 1000)
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
 job/watch-directory)
