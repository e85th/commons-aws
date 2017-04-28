(ns e85th.commons.aws.s3-test
  (:require [e85th.commons.aws.s3 :as s3]
            [clojure.test :refer :all]))

(deftest s3-url?-test
  (is (false? (s3/s3-url? "")))
  (is (false? (s3/s3-url? "foo")))
  (is (false? (s3/s3-url? "foo/bar")))
  (is (false? (s3/s3-url? "http://foo/bar")))
  (is (true? (s3/s3-url? "s3://foo/bar"))))

(deftest url->bucket+key-test
  (is (= ["bucket-name" "/path"]
         (s3/url->bucket+key "s3://bucket-name/path"))))
