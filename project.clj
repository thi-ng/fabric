(defproject thi.ng/fabric "0.1.0-SNAPSHOT"
  :description  "Signal/Collect inspired compute graph"
  :url          "http://thi.ng/fabric"
  :license      {:name "Apache Software License 2.0"
                 :url "http://www.apache.org/licenses/LICENSE-2.0"
                 :distribution :repo}
  :scm          {:name "git"
                 :url "https://github.com/thi-ng/fabric"}

  :min-lein-vesion "2.4.0"

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "0.0-3308"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]

  :profiles     {:dev {:dependencies [[criterium "0.4.3"]]
                       :plugins      [[com.cemerick/clojurescript.test "0.3.3"]
                                      [lein-cljsbuild "1.0.6"]]
                       :global-vars {*warn-on-reflection* true}
                       :jvm-opts ^:replace []
                       :aliases {"cleantest" ["do" "clean," "test," "cljsbuild" "test"]}}}

  :cljsbuild    {:builds [{:id "test"
                           :source-paths ["src" "test"]
                           :compiler {:optimizations :whitespace
                                      :pretty-print true
                                      :output-to "target/fabric-0.1.0-SNAPSHOT.js"}}]
                 :test-commands {"unit-tests" ["phantomjs" :runner "target/fabric-0.1.0-SNAPSHOT.js"]}}

  :pom-addition [:developers [:developer
                              [:name "Karsten Schmidt"]
                              [:url "http://postspectacular.com"]
                              [:timezone "0"]]])
