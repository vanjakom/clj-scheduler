(ns clj-scheduler.job.vrhovi-planina
  (:use
   clj-common.clojure)
  (:require
   [clojure.core.async :as async]
   [hiccup.core :as hiccup]

   [clj-common.2d :as draw]
   [clj-common.as :as as]
   [clj-common.context :as context]
   [clj-common.http :as http]
   [clj-common.edn :as edn]
   [clj-common.io :as io]
   [clj-common.json :as json]
   [clj-common.localfs :as fs]
   [clj-common.path :as path]
   [clj-common.pipeline :as pipeline]
   [clj-common.view :as view]

   [clj-geo.import.geojson :as geojson]
   [clj-geo.import.gpx :as gpx]
   [clj-geo.import.osm :as osm]
   [clj-geo.import.osmapi :as osmapi]
   [clj-geo.math.tile :as tile-math]
   [clj-geo.visualization.map :as map]
   
   [clj-scheduler.core :as core]
   [clj-scheduler.env :as env]))

(def dataset-path ["Users" "vanja" "dataset-cloud" "iso_planic"])

(def peak-seq
  (let [parse-coordinate-fn (fn [coordinate]
                              (let [plain (->
                                           coordinate
                                           (.replace "°" "")
                                           (.replace "⁰" "")
                                           (.replace "'" "")
                                           (.replace "‘" "")
                                           (.replace " " ""))]
                                ;; useful for debug of special chars
                                #_(println plain)
                                (let [degree (as/as-double (.substring plain 0 2))
                                      minute (as/as-double (.substring plain 2))]
                                  (+ degree (/ minute 60)))))]
    (with-open [is (fs/input-stream (path/child
                                     dataset-path
                                     "vrhovi_u_srbiji_2020.csv"))]
      (doall
       (map
        (fn [[index fields]]
          (try
            {
             :line (inc index)
             :name (nth fields 2 )
             :mountain (nth fields 3)
             :elevation (as/as-long (nth fields 4))
             :latitude (parse-coordinate-fn (nth fields 5))
             :longitude (parse-coordinate-fn (nth fields 6))
             :topo-map (nth fields 7)
             :region (nth fields 8)
             :description (when (>= (count fields) 10) (nth fields 9))}
            (catch Exception e
              (println "unable to process line at index " index " with fields " fields)
              (.printStackTrace e))))
        (filter
         #(not (contains?
                (into #{} (concat
                           (range 0 12)
                           (range 29 34)
                           (range 211 216)
                           ;; rest
                           (range 549 5000)
                           [22  35 39 50 113 137 155 158 169 200 202 207  229 239
                            253 258 285 294 302 305 309 311 324 330 336 340 347 389
                            392 394 420 429 445 459 463 482 484 517 526 528]))
                (first %)))
         (map-indexed
          (fn [index line]
            [index (into [] (.split line "\t"))])
          (io/input-stream->line-seq is))))))))

(count peak-seq) ;; 487

(with-open [os (fs/output-stream (path/child dataset-path "dataset.geojson"))]
  (json/write-to-stream
   (geojson/geojson
    (map
     (fn [peak]
       (geojson/point
        (:longitude peak)
        (:latitude peak)
        peak))
     peak-seq))
   os))

(run!
 (fn [[mountain peak-seq]]
   (println mountain)
   (doseq [peak peak-seq]
     (println "\t" (:elevation peak) (:name peak))))

 (group-by :mountain peak-seq))


(def mountain-seq
  (map
   (fn [[mountain peak-seq]]
     (let [[highest & other-peak-seq] (reverse (sort-by :elevation peak-seq))]
       {
        :name mountain
        :highest highest
        :peak-seq other-peak-seq}))
   (group-by :mountain peak-seq)))

(run!
 (fn [mountain]
   (println
    (:name mountain)
    "("
    (get-in mountain [:highest :elevation])
    (get-in mountain [:highest :name])
    ")")
   (doseq [peak (:peak-seq mountain)]
     (println "\t" (:elevation peak) (:name peak))))
 mountain-seq)

(def hut-seq
  (with-open [is (fs/input-stream (path/child dataset-path "domovi.overpass.geojson"))]
    (doall
     (map
      (fn [feature]
        {
         :longitude (get-in feature [:geometry :coordinates 0])
         :latitude (get-in feature [:geometry :coordinates 1])
         :name (get-in feature [:properties :name])})
      (:features (json/read-keyworded is))))))

(count hut-seq) ;; 97

;; TODO CHANGE TO osm-pss-integration all output paths ...
(with-open [os (fs/output-stream (path/child dataset-path "data.js"))]
  (io/write-line
   os
   (str "let mountains = " (json/write-to-string mountain-seq) "\n"))
  (io/write-line
   os
   (str "let huts = " (json/write-to-string hut-seq) "\n")))

;; will override custom code
(with-open [os (fs/output-stream (path/child dataset-path "map-clean.html"))]
  (io/write-string
   os
   (map/render-raw
    {}
    [
     (map/tile-layer-osm false)
     (map/tile-layer-opentopomap true)
     (map/tile-overlay-waymarked-hiking true)])))


#_(clojure.string/join "\n" (map #(str (first %) " = " (second %)) peak))

;; 0 (empty)
;; 1 1
;; 2 Crveni čot (vojni)
;; 3 Fruška gora
;; 4 540
;; 5 45⁰09.414‘
;; 6 19°42.569'
;; 7 378 3 Novi Sad
;; 8 Vojvodina
;; 9 Najviši vrh Južnobačkog okruga. Izohipsa iz 3D modela generisana u tom delu na 540, GPS uređajima merili kod ograde 538, dok je humka sa antenom unutra nekoliko metara viša.
