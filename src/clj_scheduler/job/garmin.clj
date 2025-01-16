(ns clj-scheduler.job.garmin
  (:require
   [clj-common.as :as as]
   [clj-common.context :as context]
   [clj-common.io :as io]
   [clj-common.localfs :as fs]
   [clj-common.path :as path]
   [clj-scheduler.core :as core]))

(defn retrieve-from-device [context]
  (let [configuration (context/configuration context)
        device-volume-name (get configuration :volume-name)
        device-track-root-path ["Volumes" device-volume-name "Garmin" "GPX"]
        device-archive-root-path ["Volumes" device-volume-name "Garmin" "GPX" "Archive"]
        device-waypoints-root-path ["Volumes" device-volume-name "Garmin" "GPX"]
        device-geocache-path ["Volumes" device-volume-name "Garmin" "geocache_visits.txt"]
        device-geocache-xml-path ["Volumes" device-volume-name "Garmin" "geocache_logs.xml"]
        
        track-root-path (get configuration :track-root-path)
        archive-root-path (get configuration :archive-root-path)
        waypoints-root-path (get configuration :waypoints-root-path)
        geocache-root-path (get configuration :geocache-root-path)]

    (context/trace context (str "device volume name: " device-volume-name))
    (context/trace context (str "track root path: " track-root-path))
    (context/trace context (str "archive root path: " archive-root-path))
    (context/trace context (str "waypoints root path: " waypoints-root-path))
    (context/trace context (str "geocache root path: " geocache-root-path))

    (if (fs/exists? ["Volumes" device-volume-name])
      (do
        (context/trace context "Volume present")
        (doseq [track (fs/list device-track-root-path)]
          (when (.startsWith (path/name track) "Track_")
            (context/trace context (str "processing track: " (path/name track)))
            (fs/move track (path/child track-root-path (path/name track)))))
        (doseq [waypoint (fs/list device-waypoints-root-path)]
          (when (.startsWith (path/name waypoint) "Waypoints_")
            (context/trace context(str "processing waypoints: " (path/name waypoint)))
            (fs/move waypoint (path/child waypoints-root-path (path/name waypoint)))))
        (doseq [archive (fs/list device-archive-root-path)]
          (context/trace context (str "processing archive: " (path/name archive)))
          (fs/move archive (path/child archive-root-path (path/name archive))))
        (when (fs/exists? device-geocache-path)
          (let [date (.format
                      (new java.text.SimpleDateFormat "yyyyMMdd")
                      (System/currentTimeMillis))
                name (str
                      (path/name-without-extension device-geocache-path)
                      "_" date "."
                      (path/extension device-geocache-path))]
            (context/trace context (str "processing geocache log as " name))
            ;; maybe better to use move but it would require geocache file update
            (fs/copy
             device-geocache-path
             (path/child geocache-root-path name))))
        (when (fs/exists? device-geocache-xml-path)
          (let [date (.format
                      (new java.text.SimpleDateFormat "yyyyMMdd")
                      (System/currentTimeMillis))
                name (str
                      (path/name-without-extension device-geocache-xml-path)
                      "_" date "."
                      (path/extension device-geocache-xml-path))]
            (context/trace context (str "processing geocache xml log as " name))
            ;; maybe better to use move but it would require geocache file update
            (fs/copy
             device-geocache-xml-path
             (path/child geocache-root-path name)))))
      (context/trace context "Volume not present, exiting") )))

#_(retrieve-from-device
 (context/create-stdout-context
  {
   :volume-name "GARMIN"
   :track-root-path (path/string->path "/Users/vanja/dataset-cloud/garmin/gpx")
   :archive-root-path (path/string->path "/Users/vanja/dataset-cloud/garmin/daily")
   :waypoints-root-path (path/string->path "/Users/vanja/dataset-cloud/garmin/waypoints")
   :geocache-root-path (path/string->path "/Users/vanja/dataset-cloud/garmin/geocache")}))
