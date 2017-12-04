(ns e85th.commons.aws.sns
  (:require [amazonica.aws.sns :as sns]
            [e85th.commons.aws.domain :as domain]
            [e85th.commons.sms :as sms]
            [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [e85th.commons.tel :as tel]))

;;----------------------------------------------------------------------
(s/fdef mk-topic
        :args (s/cat :creds ::domain/creds :topic-name string?)
        :ret string?)

(defn mk-topic
  "Create a topic name"
  [creds topic-name]
  ;; nb create-topic creates or returns the existing topic
  (:topic-arn (sns/create-topic creds :name topic-name)))

;;----------------------------------------------------------------------
(s/fdef subscribe-queue-to-topic
        :args (s/cat :creds ::domain/creds :q-arn string? :t-arn string?))

(defn subscribe-queue-to-topic
  "Subscribes the queue to the topic with raw message delivery."
  [creds q-arn t-arn]
  (let [{:keys [subscription-arn]} (sns/subscribe creds :protocol :sqs :endpoint q-arn :topic-arn t-arn)]
    ;; raw message delivery avoids SNS meta data envelope
    (sns/set-subscription-attributes creds
                                     :subscription-arn subscription-arn
                                     :attribute-name :RawMessageDelivery
                                     :attribute-value true)))

;;----------------------------------------------------------------------
(s/fdef publish
        :args (s/cat :creds ::domain/creds :topic-arn string? :msg string?))

(defn publish
  "Publishes a message to the topic arn. The msg is likely json."
  [creds topic-arn msg]
  (sns/publish creds :topic-arn topic-arn :message msg))


;;----------------------------------------------------------------------
(s/fdef send-sms
        :args (s/cat :creds ::domain/creds :to-nbr string? :msg string?))

(defn send-sms
  "Sends an SMS message. The to-nbr must be normalized to E164 format and this implementation
  will do that."
  [creds to-nbr msg]
  ;; SNS doesn't seem to deliver to numbers that are not in E164 format
  ;; During testing 2121234567 did not work but 12121234567 would, so it's
  ;; better just to have the nbr normalized
  (sns/publish creds :phone-number (tel/normalize to-nbr) :message msg))

(defrecord SnsSmsSender [creds]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  sms/ISmsSender
  (send [this {:keys [to-nbr body] :as msg}]
    (assert (seq to-nbr) )
    (send-sms creds to-nbr body)))


;;----------------------------------------------------------------------
(s/fdef new-sms-sender
        :args (s/cat :creds ::domain/creds))

(defn new-sms-sender
  "Creates a new SnsSmsSender."
  [creds]
  (map->SnsSmsSender {:creds creds}))
