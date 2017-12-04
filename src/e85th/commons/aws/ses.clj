(ns e85th.commons.aws.ses
  (:require [amazonica.aws.simpleemail :as ses]
            [e85th.commons.email :as email]
            [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log])
  (:import [com.amazonaws.services.simpleemail.model MessageRejectedException]
           [clojure.lang IFn]))

;;----------------------------------------------------------------------
(s/fdef message->destination
        :args (s/cat :msg ::email/message))

(defn- message->destination
  "Create a destination map of addresses."
  [{:keys [to cc bcc] :as msg}]
  (cond-> {:to-addresses to}
    (seq cc) (assoc :cc-addresses cc)
    (seq bcc) (assoc :bcc-addresses bcc)))

;;----------------------------------------------------------------------
(s/fdef message->body
        :args (s/cat :msg ::email/message))

(defn- message->body
  "Generates the body map."
  [{:keys [body content-type] :as msg}]
  (condp = content-type
    email/html-content-type {:html body}
    {:text body}))

;;----------------------------------------------------------------------
(s/fdef send-message
        :args (s/cat :subject-modifier-fn fn? :msg ::email/message))
(defn- send-message
  "Sends a message using ses."
  [subject-modifier-fn {:keys [from subject body content-type] :as msg}]
  (ses/send-email :destination (message->destination msg)
                  :source from
                  :message {:subject (subject-modifier-fn subject)
                            :body (message->body msg)}))

(defrecord SesEmailSender [subject-modifier-fn]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  email/IEmailSender
  (send [this msg]
    (->> msg
         (merge {:content-type email/default-content-type})
         (send-message subject-modifier-fn))))

(defn new-ses-email-sender
  "Creates a new SES email sender."
  ([]
   (new-ses-email-sender identity))
  ([subject-modifier-fn]
   (map->SesEmailSender {:subject-modifier-fn subject-modifier-fn})))
