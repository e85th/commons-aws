(ns e85th.commons.aws.sns
  (:require [amazonica.aws.sns :as sns]
            [e85th.commons.aws.models :as m]
            [e85th.commons.sms :as sms]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [e85th.commons.tel :as tel]))

(s/defn mk-topic
  "Create a topic name"
  ([topic-name :- s/Str]
   ;; nb create-topic creates or returns the existing topic
   (mk-topic m/default-profile topic-name))
  ([profile :- m/Profile topic-name :- s/Str]
   ;; nb create-topic creates or returns the existing topic
   (:topic-arn (sns/create-topic profile :name topic-name))))

(s/defn subscribe-queue-to-topic
  "Subscribes the queue to the topic with raw message delivery."
  ([q-arn :- s/Str t-arn :- s/Str]
   (subscribe-queue-to-topic m/default-profile q-arn t-arn))
  ([profile :- m/Profile q-arn :- s/Str t-arn :- s/Str]
   (let [{:keys [subscription-arn]} (sns/subscribe profile :protocol :sqs :endpoint q-arn :topic-arn t-arn)]
     ;; raw message delivery avoids SNS meta data envelope
     (sns/set-subscription-attributes profile
                                      :subscription-arn subscription-arn
                                      :attribute-name :RawMessageDelivery
                                      :attribute-value true))))

(s/defn publish
  "Publishes a message to the topic arn. The msg is likely json."
  ([topic-arn :- s/Str msg :- s/Str]
   (publish m/default-profile topic-arn msg))
  ([profile :- m/Profile topic-arn :- s/Str msg :- s/Str]
   (sns/publish profile :topic-arn topic-arn :message msg)))


(s/defn send-sms
  "Sends an SMS message. The to-nbr must be normalized to E164 format. See tel/normalize."
  ([to-nbr :- s/Str msg :- s/Str]
   (send-sms m/default-profile to-nbr msg))
  ([profile :- m/Profile to-nbr :- s/Str msg :- s/Str]
   ;; SNS doesn't seem to deliver to numbers that are not in E164 format
   ;; During testing 2121234567 did not work but 12121234567 would, so it's
   ;; better just to have the nbr normalized
   (sns/publish profile :phone-number (tel/normalize to-nbr) :message msg)))

(defrecord SnsSmsSender [profile]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  sms/ISmsSender
  (send [this {:keys [to-nbr body] :as msg}]
    (assert (seq to-nbr) )
    (send-sms profile to-nbr body)))


(s/defn new-sms-sender
  "Creates a new SnsSmsSender."
  ([]
   (new-sms-sender m/default-profile))
  ([profile :- m/Profile]
   (map->SnsSmsSender {:profile profile})))
