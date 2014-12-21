(defproject thi.ng/fabric "0.1.0-SNAPSHOT"
  :description  "Signal/Collect inspired compute graph"
  :url          "http://thi.ng/fabric"
  :license      {:name "Apache Software License 2.0"
                 :url "http://www.apache.org/licenses/LICENSE-2.0"
                 :distribution :repo}
  :scm          {:name "git"
                 :url "https://github.com/thi-ng/fabric"}

  :min-lein-vesion "2.4.0"

  :dependencies [[org.clojure/clojure "1.7.0-alpha4"]]

  :source-paths ["src/cljx"]
  :test-paths   ["target/test-classes"]

  :profiles     {:dev {:dependencies [[org.clojure/clojurescript "0.0-2496"]
                                      [criterium "0.4.3"]]
                       :plugins      [[com.keminglabs/cljx "0.5.0"]
                                      [com.cemerick/clojurescript.test "0.3.3"]
                                      [lein-cljsbuild "1.0.4-SNAPSHOT"]]
                       :global-vars {*warn-on-reflection* true}
                       :jvm-opts ^:replace []
                       :auto-clean false
                       :prep-tasks [["cljx" "once"]]
                       :aliases {"cleantest" ["do" "clean," "cljx" "once," "test," "cljsbuild" "test"]}}}

  :cljx         {:builds [{:source-paths ["src/cljx"]
                           :output-path "target/classes"
                           :rules :clj}
                          {:source-paths ["src/cljx"]
                           :output-path "target/classes"
                           :rules :cljs}
                          {:source-paths ["test/cljx"]
                           :output-path "target/test-classes"
                           :rules :clj}
                          {:source-paths ["test/cljx"]
                           :output-path "target/test-classes"
                           :rules :cljs}]}

  :cljsbuild    {:builds [{:id "test"
                           :source-paths ["target/classes" "target/test-classes"]
                           :compiler {:optimizations :whitespace
                                      :pretty-print true
                                      :output-to "target/fabric-0.1.0-SNAPSHOT.js"}}]
                 :test-commands {"unit-tests" ["phantomjs" :runner "target/fabric-0.1.0-SNAPSHOT.js"]}}

  :pom-addition [:developers [:developer
                              [:name "Karsten Schmidt"]
                              [:url "http://postspectacular.com"]
                              [:timezone "0"]]])
