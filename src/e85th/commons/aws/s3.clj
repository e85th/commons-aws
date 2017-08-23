(ns e85th.commons.aws.s3
  (:require [schema.core :as s]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3transfer]
            [e85th.commons.aws.models :as m]
            [taoensso.timbre :as log]
            [me.raynes.fs :as fs]
            [clj-time.core :as time]
            [clojure.string :as str])
  (:import [com.amazonaws.services.s3.model DeleteObjectsRequest$KeyVersion]))

(s/defn s3-url?
  [url :- s/Str]
  (str/starts-with? (str/lower-case url) "s3://"))

(s/defn url->bucket+key
  "Returns a tuple with the bucket name and object key"
  [url :- s/Str]
  (assert (s3-url? url) (str "Not an s3 url: " url))
  (let [n (count url)
        url' (subs url 5 n) ; url w/o proto "s3://"
        idx (str/index-of url' "/")]
    (if (and idx (pos? idx))
      [(subs url' 0 idx) (subs url' idx)]
      [(subs url' 0) "/"])))

(s/defn new-delete-request-key
  "Generates a new delete objects request key version."
  ([k :- s/Str]
   (DeleteObjectsRequest$KeyVersion. k))
  ([k :- s/Str v :- s/Str]
   (DeleteObjectsRequest$KeyVersion. k v)))

(s/defn ensure-no-leading-slash
  [path]
  (if (str/starts-with? path "/")
    (str/replace-first path #"/" "")
    path))

(s/defn ls
  ([bucket :- s/Str path :- s/Str]
   (ls m/default-profile bucket path))
  ([profile :- m/Profile bucket :- s/Str path :- s/Str]
   (s3/list-objects profile bucket path)))

(s/defn rm-dir
  "There really are no directories. Deleting all items with a .../key/..
  removes the directory."
  ([bucket :- s/Str path :- s/Str]
   (rm-dir m/default-profile bucket path))
  ([profile :- m/Profile bucket :- s/Str path :- s/Str]
   (let [path (ensure-no-leading-slash path)
         {:keys [object-summaries]} (s3/list-objects bucket path)
         xform (comp (map :key)
                  (map new-delete-request-key))]
     (when (seq object-summaries)
       (s3/delete-objects
        {:bucket-name bucket
         ;; keys needs to be not lazy
         :keys (into [] xform object-summaries)})))))


(s/defn exists?
  "Note this won't find directories that you see in the s3 console.  There are no directories
   it is just a ui thing.  There are only objects."
  ([bucket :- s/Str path :- s/Str]
   (exists? m/default-profile bucket path))
  ([profile :- m/Profile bucket :- s/Str path :- s/Str]
   (let [path (ensure-no-leading-slash path)]
     (s3/does-object-exist bucket path))))


(s/defn presigned-upload-url :- s/Str
  "Creates a presigned upload url to directly save to s3 using HTTP PUT.
   creds is a map with {:profile ...} or {:access-key .. :secret-key .. :endpoint ..}"
  [creds bucket-name :- s/Str key :- s/Str content-type :- s/Str expiration-seconds :- s/Int]
  (assert (not (str/starts-with? key "/")) "the key must not begin with a '/'")
  (str (s3/generate-presigned-url creds
                                  :bucket-name bucket-name
                                  :key key
                                  :method com.amazonaws.HttpMethod/PUT
                                  :content-type content-type
                                  :expiration (-> expiration-seconds time/seconds time/from-now))))
