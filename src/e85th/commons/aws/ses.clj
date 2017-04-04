(ns e85th.commons.aws.ses
  (:require [amazonica.aws.simpleemail :as ses]
            [e85th.commons.email :as email]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [taoensso.timbre :as log])
  (:import [com.amazonaws.services.simpleemail.model MessageRejectedException]
           [clojure.lang IFn]))

(s/defn ^:private message->destination
  "Create a destination map of addresses."
  [{:keys [to cc bcc]} :- email/Message]
  (cond-> {:to-addresses to}
    (seq cc) (assoc :cc-addresses cc)
    (seq bcc) (assoc :bcc-addresses bcc)))

(s/defn ^:private message->body
  "Generates the body map."
  [{:keys [body content-type]} :- email/Message]
  (condp = content-type
    email/html-content-type {:html body}
    {:text body}))

(s/defn ^:private send-message
  "Sends a message using ses."
  [subject-modifier-fn :- IFn {:keys [from subject body content-type] :as msg} :- email/Message]
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

(s/defn new-ses-email-sender
  "Creates a new SES email sender."
  ([]
   (new-ses-email-sender identity))
  ([subject-modifier-fn :- IFn]
   (map->SesEmailSender {:subject-modifier-fn subject-modifier-fn})))
