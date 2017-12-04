(ns e85th.commons.aws.sqs
  (:require [amazonica.aws.sqs :as sqs]
            [clojure.spec.alpha :as s]
            [e85th.commons.aws.domain :as domain]
            [e85th.commons.aws.sns :as sns]
            [e85th.commons.util :as u]
            [clojure.string :as str]
            [cheshire.core :as json]))

;;----------------------------------------------------------------------
(s/fdef ls
        :args (s/cat :creds ::domain/creds)
        :ret (s/coll-of string?))

(defn ls
  "Lists the queues"
  [creds]
  (:queue-urls (sqs/list-queues creds)))

;;----------------------------------------------------------------------
(s/fdef mk
        :args (s/cat :creds ::domain/creds :q-name string?)
        :ret string?)

(defn mk
  "Creates a queue with name q-name and if specified a creds. If the queue already exists
   nothing is done but the queue url is still returned."
  [creds q-name]
  (:queue-url (sqs/create-queue creds :queue-name q-name)))

;;----------------------------------------------------------------------
(s/fdef rm
        :args (s/cat :creds ::domain/creds :q-url string?))

(defn rm
  "Remove the queue specified by the q-url."
  [creds q-url]
  (sqs/delete-queue creds q-url))


;;----------------------------------------------------------------------
(s/fdef name->url
        :args (s/cat :creds ::domain/creds :q-name string?)
        :ret (s/nilable string?))

(defn name->url
  "Looks up a queue url "
  [creds q-name]
  (sqs/find-queue creds q-name))

;;----------------------------------------------------------------------
(s/fdef url->attrs
        :args (s/cat :creds ::domain/creds :q-url string?))

(defn url->attrs
  "Answers with all attrs for a given queue url"
  [creds q-url]
  (sqs/get-queue-attributes creds q-url ["All"]))

;;----------------------------------------------------------------------
(s/fdef url->arn
        :args (s/cat :creds ::domain/creds :q-url string?)
        :ret string?)

(defn url->arn
  [creds q-url]
  (:QueueArn (url->attrs creds q-url)))

;;----------------------------------------------------------------------
(s/fdef name->arn
        :args (s/cat :creds ::domain/creds :q-name string?)
        :ret (s/nilable string?))

(defn name->arn
  [creds q-name]
  (some->> q-name (name->url creds) (url->arn creds)))

;;----------------------------------------------------------------------
(s/fdef enqueue
        :args (s/cat :creds ::domain/creds :q-url string? :msg some?)
        :ret (s/nilable string?))

(defn enqueue
  [creds q-url msg]
  (sqs/send-message creds q-url msg))


;;----------------------------------------------------------------------
(s/fdef dequeue
        :args (s/cat :creds ::domain/creds :q-url string? :max-messages nat-int? :wait-secs nat-int?)
        :ret (s/nilable string?))

(defn dequeue
  "Dequeues a message from the queue specified by q-url.  The message is not implicitly
   deleted from the queue.  wait-secs should generally be 20 (seconds). If there is
   a message, this method will return sooner than wait-secs seconds. max-messages is the max number of messages
   to dequeue at once. This is potentially a blocking call if there are no messages to be dequeued."
  [creds q-url max-messages wait-secs]
  (sqs/receive-message creds :queue-url q-url :delete false :wait-time-seconds wait-secs :max-number-of-messages max-messages))

;;----------------------------------------------------------------------
(s/fdef delete-message
        :args (s/cat :creds ::domain/creds :q-url string? :msg some?))

(defn delete-message
  [creds q-url msg]
  (sqs/delete-message creds (assoc msg :queue-url q-url)))

;;----------------------------------------------------------------------
(s/fdef return-to-queue
        :args (s/cat :creds ::domain/creds :q-url string? :msg some?))

(defn return-to-queue
  "Returns a message to the queue."
  [creds q-url msg]
  (sqs/change-message-visibility creds (merge msg {:queue-url q-url :visibility-timeout 0})))

;;----------------------------------------------------------------------
(s/fdef gen-subscribe-policy
        :args (s/cat :q-arn string? :topic-arns (s/coll-of string?))
        :ret map?)

(defn- gen-subscribe-policy
  "Generates a policy which allows the queue identified by q-arn to subscribe to
   the topic-arns."
  [q-arn topic-arns]
  {:Version "2012-10-17"
   :Statement [{:Sid "sqs-sns-subscribe"
                :Effect "Allow"
                :Principal "*"
                :Action "sqs:SendMessage"
                :Resource q-arn
                :Condition {:ArnEquals {:aws:SourceArn topic-arns}}}]})

;;----------------------------------------------------------------------
(s/fdef subscribe-to-topics
        :args (s/cat :creds ::domain/creds :q-url string? :topic-names (s/coll-of string?))
        :ret string?)

(defn subscribe-to-topics
  "Subscribes sqs queue q-url to topics. Answers with the q-url for q-name."
  [creds q-url topic-names]
  (let [q-arn (url->arn creds q-url)
        topic-arns (map (partial sns/mk-topic creds) topic-names)
        q-policy (json/generate-string (gen-subscribe-policy q-arn topic-arns))]
    (sqs/set-queue-attributes creds :queue-url q-url :attributes {"Policy" q-policy})
    (run! (partial sns/subscribe-queue-to-topic creds q-arn) topic-arns)
    q-url))


;;----------------------------------------------------------------------
(s/fdef assign-dead-letter-queue
        :args (s/cat :creds ::domain/creds :q-url string? :dlq-url string? :max-receive-count nat-int?))

(defn- assign-dead-letter-queue
  [creds q-url dlq-url max-receive-count]
  (let [dlq-arn (url->arn creds dlq-url)
        policy (json/generate-string {:maxReceiveCount max-receive-count
                                      :deadLetterTargetArn dlq-arn})]
    (sqs/set-queue-attributes creds q-url {"RedrivePolicy" policy})))

;;----------------------------------------------------------------------
(s/fdef mk-with-redrive-policy
        :args (s/cat :creds ::domain/creds :q-name string? :dlq-name string?)
        :ret string?)

(defn mk-with-redrive-policy
  "Makes a queue with a redrive policy attached. Returns the q-url.
  Creates a dead letter queue with name dlq-name which is where failed
  messages will end up in when a message is returned to the queue."
  [creds q-name dlq-name]
  (let [q-url (mk creds q-name)
        dlq-url (mk creds dlq-name)]
    (assign-dead-letter-queue creds q-url dlq-url 1)
    q-url))


;;----------------------------------------------------------------------
(s/fdef gen-dynamic-name
        :args (s/cat :env string? :context string?)
        :ret string?)

(defn- gen-dynamic-name
  "Generates a queue name based on the env and context. "
  [env context]
  (let [hostname (str/replace (u/hostname) #"\." "-") ;; sqs names can't have "."
        queue-name (format "%s-%s-%s" env context hostname)
        str-len (min 80 (count queue-name))] ;; 80 is max queue name length
    (subs queue-name 0 str-len)))

;;----------------------------------------------------------------------
(s/fdef enqueue
        :args (s/cat :creds ::domain/creds :q-url string? :msg some?))

(defn enqueue
  [creds q-url msg]
  (sqs/send-message creds q-url msg))
