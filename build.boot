(set-env!
 :source-paths #{"test"}
 :resource-paths #{"src"}
 :dependencies '[[org.clojure/clojure "1.9.0-alpha17" :scope "provided"]
                 [e85th/commons "0.1.24"]
                 [amazonica "0.3.105" :scope "provided"]
                 [adzerk/boot-test "1.2.0" :scope "test"]]

 :repositories #(conj %
                      ["clojars" {:url "https://clojars.org/repo"
                                  :username (System/getenv "CLOJARS_USER")
                                  :password (System/getenv "CLOJARS_PASS")}]))

(require '[adzerk.boot-test :as boot-test])

(deftask test
  "Runs the unit-test task"
  []
  (comp
   (javac)
   (boot-test/test)))


(deftask build
  "Builds a jar for deployment."
  []
  (comp
   (javac)
   (pom)
   (jar)
   (target)))

(deftask dev
  "Starts the dev task."
  []
  (comp
   (repl)
   (watch)))

(deftask deploy
  []
  (comp
   (build)
   (push)))

(task-options!
 pom {:project 'e85th/commons-aws
      :version "0.1.4"
      :description "AWS implementation of protocols in commons"
      :url "http://github.com/e85th/commons-aws"
      :scm {:url "http://github.com/e85th/commons-aws"}
      :license {"Apache License 2.0" "http://www.apache.org/licenses/LICENSE-2.0"}}
 push {:repo "clojars"})
