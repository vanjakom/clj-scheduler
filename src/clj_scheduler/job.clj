(ns clj-scheduler.job
  (:require
   [clj-common.path :as path]
   [clj-common.localfs :as fs]
   [clj-scheduler.core :as core]))

(defn hello-world [context]
  (core/context-report context "hello world")
  (core/context-counter context "hello")
  (Thread/sleep 1000)
  (core/context-counter context "hello")
  (core/context-report context "end"))

;; todo, maybe better to use create functions like for trigger

(defn watch-directory [context]
  (let [configuration (core/context-configuration context)
        directory (get configuration :directory)
        ;; called for each file in directory, getting path
        ;; if true file should be considered
        ;; default match all files
        match-fn (or
                  (get configuration :match-fn)
                  (constantly true))
        ;; given configuration and sequence of all matched files
        ;; returns sequence of files which should be deleted
        ;; default prevents delete
        delete-fn (or
                   (get configuration :delete-fn)
                   (constantly false))
        ;; if true files will be just reported, not actually deleted
        ;; default false
        dry-run (get configuration :dry-run)
        files (fs/list directory)]
    (core/context-report context "present files:")
    (doseq [file files]
      (core/context-report context (path/path->string file)))
    (let [matched-files (filter match-fn files)]
      (core/context-report context "matched files:")
      (doseq [file matched-files]
        (core/context-report context (path/path->string file)))
      (let [delete-files (delete-fn matched-files)]
        (doseq [file delete-files]
          (if dry-run
            (core/context-report
             context
             (str "would delete: " (path/path->string file)))
            (do
              (fs/delete file)
              (core/context-report
               context
               (str "deleted: " (path/path->string file))))))))))


;; test directory watcher
#_(let [match-fn (fn [file]
                 (let [name (last file)]
                   (when (.startsWith name "planet-notes")
                     (let [date (->
                                 name
                                 (.replace "planet-notes-" "")
                                 (.replace ".osn.bz2" ""))]
                       (when-not (= date "latest")
                         date)))))]
  (core/job-sumbit
   (core/job-create
    "cleanup /tmp/test-dir"
    {
     :directory (path/string->path "/tmp/test-dir")
     :match-fn match-fn
     :delete-fn (fn [file-seq]
                  (drop 3 (reverse (sort-by match-fn file-seq))))
     :dry-run false}
    watch-directory))) 
;; mkdir /tmp/test-dir
;; cd /tmp/test-dir
;; touch 'planet-notes-20230215 024600.osn.bz2'
