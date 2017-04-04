(ns e85th.commons.aws.sqs
  (:require [amazonica.aws.sqs :as sqs]
            [schema.core :as s]
            [e85th.commons.aws.sns :as sns]
            [e85th.commons.aws.models :as m]
            [e85th.commons.util :as u]
            [clojure.string :as string]
            [cheshire.core :as json]))

(s/defn ls :- [s/Str]
  "Lists the queues"
  ([]
   (ls m/default-profile))
  ([profile :- m/Profile]
   (:queue-urls (sqs/list-queues profile))))

(s/defn mk :- s/Str
  "Creates a queue with name q-name and if specified a profile. If the queue already exists
   nothing is done but the queue url is still returned."
  ([q-name :- s/Str]
   (mk m/default-profile q-name))
  ([profile :- m/Profile q-name :- s/Str]
   (:queue-url (sqs/create-queue profile :queue-name q-name))))

(s/defn rm
  "Remove the queue specified by the q-url."
  ([q-url :- s/Str]
   (rm m/default-profile q-url))
  ([profile :- m/Profile q-url :- s/Str]
   (sqs/delete-queue profile q-url)))


(s/defn name->url :- (s/maybe s/Str)
  "Looks up a queue url "
  ([q-name :- s/Str]
   (name->url m/default-profile q-name))
  ([profile :- m/Profile q-name :- s/Str]
   (sqs/find-queue profile q-name)))

(s/defn url->attrs
  "Answers with all attrs for a given queue url"
  ([q-url :- s/Str]
   (url->attrs m/default-profile q-url))
  ([profile :- m/Profile q-url :- s/Str]
   (sqs/get-queue-attributes profile q-url ["All"])))

(s/defn url->arn :- s/Str
  ([q-url :- s/Str]
   (url->arn m/default-profile q-url))
  ([profile :- m/Profile q-url :- s/Str]
   (:QueueArn (url->attrs profile q-url))))

(s/defn name->arn :- (s/maybe s/Str)
  ([q-name :- s/Str]
   (name->arn m/default-profile q-name))
  ([profile :- m/Profile q-name :- s/Str]
   (some->> q-name (name->url profile) (url->arn profile))))

(s/defn enqueue
  ([q-url :- s/Str msg]
   (enqueue m/default-profile q-url msg))
  ([profile :- m/Profile q-url :- s/Str msg]
   (sqs/send-message profile q-url msg)))


(s/defn dequeue
  "Dequeues a message from the queue specified by q-url.  The message is not implicitly
   deleted from the queue.  wait-secs should generally be 20 (seconds). If there is
   a message, this method will return sooner than wait-secs seconds. max-messages is the max number of messages
   to dequeue at once. This is potentially a blocking call if there are no messages to be dequeued."
  ([q-url :- s/Str max-messages :- s/Int wait-secs :- s/Int]
   (dequeue m/default-profile q-url max-messages wait-secs))
  ([profile :- m/Profile q-url :- s/Str max-messages :- s/Int wait-secs :- s/Int]
   (sqs/receive-message profile :queue-url q-url :delete false :wait-time-seconds wait-secs :max-number-of-messages max-messages)))


(s/defn delete-message
  ([q-url :- s/Str msg]
   (delete-message m/default-profile q-url msg))
  ([profile :- m/Profile q-url :- s/Str msg]
   (sqs/delete-message profile (assoc msg :queue-url q-url))))

(s/defn return-to-queue
  "Returns a message to the queue."
  ([q-url :- s/Str msg]
   (return-to-queue m/default-profile q-url msg))
  ([profile :- m/Profile q-url :- s/Str msg]
   (sqs/change-message-visibility profile (merge msg {:queue-url q-url :visibility-timeout 0}))))

(s/defn ^:private gen-subscribe-policy
  "Generates a policy which allows the queue identified by q-arn to subscribe to
   the topic-arns."
  [q-arn :- s/Str topic-arns :- [s/Str]]
  {:Version "2012-10-17"
   :Statement [{:Sid "sqs-sns-subscribe"
                :Effect "Allow"
                :Principal "*"
                :Action "sqs:SendMessage"
                :Resource q-arn
                :Condition {:ArnEquals {:aws:SourceArn topic-arns}}}]})

(s/defn subscribe-to-topics :- s/Str
  "Subscribes sqs queue q-url to topics. Answers with the q-url for q-name."
  ([q-url :- s/Str topic-names :- [s/Str]]
   (subscribe-to-topics m/default-profile q-url topic-names))
  ([profile :- m/Profile q-url :- s/Str topic-names :- [s/Str] ]
   (let [q-arn (url->arn profile q-url)
         topic-arns (map (partial sns/mk-topic profile) topic-names)
         q-policy (json/generate-string (gen-subscribe-policy q-arn topic-arns))]
     (sqs/set-queue-attributes profile :queue-url q-url :attributes {"Policy" q-policy})
     (run! (partial sns/subscribe-queue-to-topic profile q-arn) topic-arns)
     q-url)))


(s/defn ^:private assign-dead-letter-queue
  ([q-url :- s/Str dlq-url :- s/Str max-receive-count :- s/Int]
   (assign-dead-letter-queue m/default-profile q-url dlq-url max-receive-count))
  ([profile :- m/Profile q-url :- s/Str dlq-url :- s/Str max-receive-count :- s/Int]
   (let [dlq-arn (url->arn profile dlq-url)
         policy (json/generate-string {:maxReceiveCount max-receive-count
                                       :deadLetterTargetArn dlq-arn})]
     (sqs/set-queue-attributes profile q-url {"RedrivePolicy" policy}))))

(s/defn mk-with-redrive-policy :- s/Str
  "Makes a queue with a redrive policy attached. Returns the q-url.
  Creates a dead letter queue with name dlq-name which is where failed
  messages will end up in when a message is returned to the queue."
  ([q-name :- s/Str dlq-name :- s/Str]
   (mk-with-redrive-policy m/default-profile q-name dlq-name))
  ([profile :- m/Profile q-name :- s/Str dlq-name :- s/Str]
   (let [q-url (mk profile q-name)
         dlq-url (mk profile dlq-name)]
     (assign-dead-letter-queue profile q-url dlq-url 1)
     q-url)))


(s/defn gen-dynamic-name :- s/Str
  "Generates a queue name based on the env and context. "
  [env :- s/Str context :- s/Str]
  (let [hostname (string/replace (u/hostname) #"\." "-") ;; sqs names can't have "."
        queue-name (format "%s-%s-%s" env context hostname)
        str-len (min 80 (count queue-name))] ;; 80 is max queue name length
    (subs queue-name 0 str-len)))

(s/defn enqueue
  ([q-url :- s/Str msg]
   (enqueue m/default-profile q-url msg))
  ([profile :- m/Profile q-url :- s/Str msg]
   (sqs/send-message profile q-url msg)))
