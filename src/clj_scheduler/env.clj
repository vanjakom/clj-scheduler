(ns clj-scheduler.env
  (:require
   [clj-common.jvm :as jvm]
   [clj-common.path :as path]))

(def http-server-port 7076)

#_(jvm/set-environment-variable
 "CLJ-SCHEDULER-STATE-PATH"
 "/Users/vanja/dataset-local/clj-scheduler/state.edn")

(def state-path (path/string->path
                 (or
                  (jvm/environment-variable "CLJ_SCHEDULER_STATE_PATH")
                  "/home/ec2-user/clj-scheduler-state.edn")))
#_(println state-path)

(def dataset-local-path
  (path/string->path
   (or (jvm/environment-variable "DATASET_LOCAL") "/Users/vanja/dataset-local")))

(def dataset-cloud-path
  (path/string->path
   (or (jvm/environment-variable "DATASET_CLOUD") "/Users/vanja/dataset-cloud")))

(def dataset-git-path
  (path/string->path
   (or (jvm/environment-variable "DATASET_GIT") "/Users/vanja/dataset-git")))

(def projects-path
  (path/string->path
   (or (jvm/environment-variable "PROJECTS_GIT") "/Users/vanja/projects")))
