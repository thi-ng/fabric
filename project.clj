(defproject signalcollect "0.1.0-SNAPSHOT"
  :description  "Signal/Collect inspired graph computations"
  :url          "https://github.com/postspectacular/sc-dev"
  :license      {:name "Apache Software License 2.0"
                 :url "http://www.apache.org/licenses/LICENSE-2.0"
                 :distribution :repo}
  :scm          {:name "git"
                 :url "git@github.com:postspectacular/sc-dev.git"}

  :min-lein-vesion "2.4.0"

  :dependencies [[org.clojure/clojure "1.6.0"]]

  :profiles     {:dev {:dependencies [[org.clojure/clojurescript "0.0-2411"]
                                      [criterium "0.4.3"]]
                       :global-vars {*warn-on-reflection* true}
                       :jvm-opts ^:replace []}}

  :pom-addition [:developers [:developer
                              [:name "Karsten Schmidt"]
                              [:url "http://postspectacular.com"]
                              [:timezone "0"]]])
