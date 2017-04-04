(defproject e85th/commons-aws "0.1.0"
  :description "AWS implementation of protocols in commons"
  :url "http://github.com/e85th/commons-aws"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha15" :scope "provided"]
                 [e85th/commons "0.1.11"]
                 ;; -- AWS
                 [amazonica "0.3.66" :scope "provided"]]

  :source-paths ["src"]
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]])
