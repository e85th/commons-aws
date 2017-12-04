(ns e85th.commons.aws.mq
  (:require [e85th.commons.mq :as mq]
            [e85th.commons.util :as u]
            [e85th.commons.aws.domain :as domain]
            [e85th.commons.aws.sqs :as sqs]
            [e85th.commons.aws.sns :as sns]
            [e85th.commons.aws.models :as m]
            [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log]
            [cheshire.core :as json]))

;(remove-ns (ns-name *ns*))

(defn- with-error-handling
  [f error-msg]
  (try
    (f)
    (catch Exception ex
      (log/error ex error-msg))))

(defn- dispatchable
  [[msg-type data]]
  [(keyword msg-type) data])

;;----------------------------------------------------------------------
(s/fdef process-message
        :args (s/cat :creds ::domain/creds :q-url string? :on-message-fn fn? :msg map?))

(defn- process-message
  "Process one message, parses it to a clojure data structure and dispatches via on-message-fn."
  [creds q-url on-message-fn {:keys [body] :as msg}]
  (try
    (let [dispatchable-msg (-> body (json/parse-string true) dispatchable)]
      (log/debugf "Message received: %s" dispatchable-msg)
      (on-message-fn dispatchable-msg)
      (sqs/delete-message creds q-url msg))
    (catch Exception ex
      (log/error ex)
      (with-error-handling #(sqs/return-to-queue creds q-url msg) "Error returning message to queue."))))

;;----------------------------------------------------------------------
(defn- run-sqs-loop*
  [creds q-url on-message-fn]
  (while true
    (let [{:keys [messages]} (sqs/dequeue creds q-url 10 20)] ;; blocking call
      (run! (partial process-message creds q-url on-message-fn) messages))))

(defn- run-sqs-loop
  [quit creds q-url on-message-fn]
  (while (not @quit)
    (with-error-handling #(run-sqs-loop* creds q-url on-message-fn) "Exception encountered, continuing with sqs-loop..")))

;;----------------------------------------------------------------------
(s/fdef mk-queue
        :args (s/cat :creds ::domain/creds :q-name string? :dlq-suffix (s/? string?))
        :ret string?)

(defn- mk-queue
  "Makes a queue dynamically with a redrive policy for failed messages.
   Answers with the q-url."
  ([creds q-name]
   (mk-queue creds q-name "-failed"))
  ([creds q-name dlq-suffix]
   (let [dlq-name (str q-name dlq-suffix)]
     (sqs/mk-with-redrive-policy creds q-name dlq-name))))

(defrecord SqsMessageProcessor [thread-name q-name topic-names creds on-message-fn resources]
  component/Lifecycle
  (start [this]
    (log/infof "SqsMessageProcessor starting for queue %s" q-name)
    (let [quit (volatile! false)
          q-url (mk-queue creds q-name)]

      (when (seq topic-names)
        (sqs/subscribe-to-topics creds q-url topic-names))

      (u/start-user-thread thread-name #(run-sqs-loop quit creds q-url (partial on-message-fn resources)))
      (assoc this :quit quit)))

  (stop [this]
    (log/infof "SqsMessageProcessor stopping for queue %s" q-name)
    (some-> this :quit (vreset! true))
    this))


(s/def ::q-name string?)
(s/def ::on-message-fn fn?)
(s/def ::creds ::domain/creds)
(s/def ::topic-name string?)
(s/def ::topic-names (s/coll-of ::topic-name))
(s/def ::thread-name string?)
(s/def ::dynamic? boolean?)
(s/def ::resources any?) ; resources map from component for example


(s/def ::message-processor-params
  (s/keys :req-un [::q-name ::on-message-fn ::creds]
          :opt-un [::topic-names ::dynamic? ::resources]))

;;----------------------------------------------------------------------
(s/fdef new-message-processor
        :args (s/cat :params ::message-processor-params))

(defn new-message-processor
  "Creates a new SqsMessageProcessor component.
   :resources are components needed by the message processing function.
   :on-message-fn is a function that takes resources and msg (on-message-fn resources msg)
    The msg passed to on-message-fn will be of [msg-type data].  msg-type is a keyword and
    can be used to dispatch via a multi method."
  [{:keys [q-name] :as params}]
  (map->SqsMessageProcessor
   (merge {:topic-names []
           :thread-name (str "sqs-mq-processor-" q-name)} params)))


(defrecord MessagePublisher [topic-name creds]
  component/Lifecycle
  (start [this]
    (log/infof "MessagePublisher starting for topic: %s" topic-name)
    (assoc this :t-arn (sns/mk-topic creds topic-name)))
  (stop [this] this)

  mq/IMessagePublisher
  (publish [this msg]
    (s/assert ::mq/message msg)
    (sns/publish creds (:t-arn this) (json/generate-string msg))))

(s/def ::message-publisher-params
  (s/keys :req-un [::topic-name ::creds]))

;;----------------------------------------------------------------------
(s/fdef new-message-publisher
        :args (s/cat :params ::message-publisher-params))

(defn new-message-publisher
  [params]
  (map->MessagePublisher params))
