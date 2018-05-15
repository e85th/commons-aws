(defproject e85th/commons-aws "0.1.6-alpha4"
  :description "AWS implementation of protocols in commons"
  :url "http://github.com/e85th/commons-aws"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [e85th/commons "0.1.29-alpha8"]
                 ;; -- AWS
                 [amazonica "0.3.111" :scope "provided"]]

  :source-paths ["src"]
  :deploy-repositories [["releases"  {:sign-releases false :url "https://clojars.org/repo"}]
                        ["snapshots" {:sign-releases false :url "https://clojars.org/repo"}]]


  ;; only to quell lein-cljsbuild when using checkouts
  :cljsbuild {:builds []})
