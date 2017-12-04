(ns e85th.commons.aws.s3
  (:require [clojure.spec.alpha :as s]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3transfer]
            [e85th.commons.aws.domain :as domain]
            [taoensso.timbre :as log]
            [me.raynes.fs :as fs]
            [clj-time.core :as time]
            [clojure.string :as str])
  (:import [com.amazonaws.services.s3.model DeleteObjectsRequest$KeyVersion]))

;;----------------------------------------------------------------------
(s/fdef s3-url?
        :args (s/cat :url string?)
        :ret boolean?)

(defn s3-url?
  [url]
  (str/starts-with? (str/lower-case url) "s3://"))


;;----------------------------------------------------------------------
(s/fdef url->bucket+key
        :args (s/cat :url string?)
        :ret (s/tuple string? string?))

(defn url->bucket+key
  "Returns a tuple with the bucket name and object key"
  [url]
  (assert (s3-url? url) (str "Not an s3 url: " url))
  (let [n (count url)
        url' (subs url 5 n) ; url w/o proto "s3://"
        idx (str/index-of url' "/")]
    (if (and idx (pos? idx))
      [(subs url' 0 idx) (subs url' idx)]
      [(subs url' 0) "/"])))


;;----------------------------------------------------------------------
(s/fdef new-delete-request-key
        :args (s/cat :k string? :v (s/? string?)))

(defn- new-delete-request-key
  "Generates a new delete objects request key version."
  ([k]
   (DeleteObjectsRequest$KeyVersion. k))
  ([k v]
   (DeleteObjectsRequest$KeyVersion. k v)))

(defn- ensure-no-leading-slash
  [path]
  (if (str/starts-with? path "/")
    (str/replace-first path #"/" "")
    path))

;;----------------------------------------------------------------------
(s/fdef ls
        :args (s/cat :creds ::domain/creds :bucket string? :path string?))

(defn ls
  [creds bucket path]
  (s3/list-objects creds bucket path))

;;----------------------------------------------------------------------
(s/fdef rm-dir
        :args (s/cat :creds ::domain/creds :bucket string? :path string?))

(defn rm-dir
  "There really are no directories. Deleting all items with a .../key/..
  removes the directory."
  [creds bucket path]
  (let [path (ensure-no-leading-slash path)
        {:keys [object-summaries]} (s3/list-objects bucket path)
        xform (comp (map :key)
                 (map new-delete-request-key))]
    (when (seq object-summaries)
      (s3/delete-objects
       {:bucket-name bucket
        ;; keys needs to be not lazy
        :keys (into [] xform object-summaries)}))))


;;----------------------------------------------------------------------
(s/fdef exists?
        :args (s/cat :creds ::domain/creds :bucket string? :path string?))

(defn exists?
  "Note this won't find directories that you see in the s3 console.  There are no directories
   it is just a ui thing.  There are only objects."
  [creds bucket path]
  (let [path (ensure-no-leading-slash path)]
    (s3/does-object-exist bucket path)))


;;----------------------------------------------------------------------
(s/fdef presigned-upload-url
        :args (s/cat :creds ::domain/creds :bucket-name string? :key string? :content-type string? :expiration-seconds nat-int?)
        :ret string?)

(defn presigned-upload-url
  "Creates a presigned upload url to directly save to s3 using HTTP PUT.
   creds is a map with {:creds ...} or {:access-key .. :secret-key .. :endpoint ..}"
  [creds bucket-name key content-type expiration-seconds]
  (assert (not (str/starts-with? key "/")) "the key must not begin with a '/'")
  (str (s3/generate-presigned-url creds
                                  :bucket-name bucket-name
                                  :key key
                                  :method com.amazonaws.HttpMethod/PUT
                                  :content-type content-type
                                  :expiration (-> expiration-seconds time/seconds time/from-now))))
