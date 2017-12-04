(ns e85th.commons.aws.domain
  (:require [clojure.spec.alpha :as s]))


(s/def ::access-key string?)
(s/def ::secret-key string?)
(s/def ::endpoint   string?)

(s/def ::profile string?)

(s/def ::aws-profile (s/keys :req-un [::profile]))

(s/def ::aws-creds (s/keys :req-un [::access-key ::secret-key]
                           :opt-un [::endpoint]))


(s/def ::creds (s/or :profile ::aws-profile
                     :creds   ::aws-creds))



;; (def promotional-sms-type "Promotional")
;; (def transactional-sms-type "Transactional")
;; (def SmsType (s/enum promotional-sms-type transactional-sms-type))

;; (s/defschema SmsOptions
;;   {(s/optional-key :sender-id) s/Str
;;    (s/optional-key :max-price) s/Num
;;    (s/optional-key :sms-type) SmsType})

;; (def default-profile
;;   {:profile "default"})
