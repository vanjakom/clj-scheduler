(ns clj-scheduler.job.system
  (:require
   [clj-common.as :as as]
   [clj-common.git :as git]
   [clj-common.io :as io]
   [clj-common.localfs :as fs]
   [clj-common.path :as path]
   [clj-scheduler.core :as core]))

(defn job-cleanup [context]
  (let [keep-last (or (as/as-long
                       (get (core/context-configuration context) :keep-last))
                      10)]
    (core/context-report context (str "keeping last " keep-last " jobs") )
    (core/jobs-cleanup keep-last)
    (core/context-report context "jobs cleanup finished")))

;; todo, maybe better to use create functions like for trigger

(defn ensure-directory [context]
  (let [configuration (core/context-configuration context)
        directory (get configuration :directory)]
    (if (fs/exists? directory)
      (core/context-report
       context
       (str "Directory present: " (path/path->string directory)))
      (do
        (fs/mkdirs directory)
        (core/context-report
         context
         (str "Directory created: " (path/path->string directory)))))))

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

;; note: there is also clj-common.jvm/execute-command which could be used
;; for workflows creation
(defn execute-command [context]
  (let [configuration (core/context-configuration context)
        pwd (get configuration :pwd)
        command (get configuration :command)]
    (core/context-report context (str "pwd for command: " pwd))
    (core/context-report context (str "executing command: " command))
    (let [process (.exec (Runtime/getRuntime)
                         command
                         (into-array java.lang.String [])
                         (new java.io.File pwd))
          is (.getInputStream process)]
      ;; wait process to finish to collect output
      (.waitFor process)
      (let [output (io/input-stream->line-seq is)]
        (doseq [line output]
          (core/context-report context line))))))

(defn git-status-repo-root
  "Checks all subdirs in given repo-root, assuming they are git repos.
  Reports status of each in state"
  [context]
  (let [configuration (core/context-configuration context)
        repo-root (get configuration :repo-root)
        state-root (get configuration :state-root)]
    (core/context-report context (str "git repo root: " (path/path->string repo-root)))
    (core/context-report context (str "state root: " state-root))
    (doseq [repo (fs/list repo-root)]
      (when (fs/is-directory repo)
        #_(core/context-report context (str "checking: " (last repo)))
        (let [status (git/status repo)]
          (core/context-report context (str "[" (name (:status status)) "] " (last repo)))
          (core/state-set
           (concat state-root [(last repo) "status"])
           (:status status)))))
    (core/context-report context "finished")))

#_(core/job-sumbit
 (core/job-create
  "test"
  {
   :repo-root ["Users" "vanja" "projects"]
   :state-root ["git" "projects"]}
  git-status-repo-root))

#_(core/job-sumbit
 (core/job-create
  "test"
  {
   :pwd "/Users/vanja/"
   :command "ps -ax"}
  execute-command))

#_(core/job-sumbit
 (core/job-create
  "test"
  {
   :pwd "/Users/vanja/"
   :command "ls -lh /users/vanja"}
  execute-command))

#_(core/job-sumbit
 (core/job-create
  "test"
  {
   :pwd "/Users/vanja/projects/notes"
   :command "git status"}
  execute-command))

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
