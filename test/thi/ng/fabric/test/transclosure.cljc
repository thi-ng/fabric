(ns thi.ng.fabric.test.transclosure
  #?(:cljs
     (:require-macros
      [cljs.core.async.macros :refer [go go-loop]]))
  (:require
   [thi.ng.fabric.core :as f]
   [thi.ng.fabric.utils :as fu]
   #?@(:clj
       [[clojure.test :refer :all]
        [clojure.core.async :refer [go go-loop chan close! <! <!! >!]]]
       :cljs
       [[cemerick.cljs.test :refer-macros [is deftest with-test testing done]]
        [cljs.core.async :refer [chan close! <! >! take!]]])))

#?(:clj (taoensso.timbre/set-level! :warn))

;; test excerpt of top-level classes of phylogenetic tree of life
;; http://tolweb.org/Life_on_Earth/1

(def types
  '[alive eubacteria eukaryotes archaea cyanobacteria xenococcus rivularia
    archaeplastida green-plants rhodophyta embryophytes spermatopsida polypodiopsida conifers
    unikonts opisthokonts animals fungi amoebozoa]
  )

(def hierarchy
  '[[alive eubacteria]
    [alive eukaryotes]
    [alive archaea]
    [eubacteria cyanobacteria]
    [cyanobacteria xenococcus]
    [cyanobacteria rivularia]
    [eukaryotes archaeplastida]
    [archaeplastida green-plants]
    [archaeplastida rhodophyta]
    [green-plants embryophytes]
    [embryophytes spermatopsida]
    [embryophytes polypodiopsida]
    [spermatopsida conifers]
    [eukaryotes unikonts]
    [unikonts opisthokonts]
    [opisthokonts animals]
    [opisthokonts fungi]
    [unikonts amoebozoa]])

(def expected-closure
  '[[0 #{alive}]
    [1 #{eubacteria alive}]
    [2 #{eukaryotes alive}]
    [3 #{archaea alive}]
    [4 #{eubacteria cyanobacteria alive}]
    [5 #{eubacteria xenococcus cyanobacteria alive}]
    [6 #{eubacteria rivularia cyanobacteria alive}]
    [7 #{archaeplastida eukaryotes alive}]
    [8 #{archaeplastida eukaryotes green-plants alive}]
    [9 #{archaeplastida eukaryotes alive rhodophyta}]
    [10 #{archaeplastida eukaryotes green-plants alive embryophytes}]
    [11 #{archaeplastida eukaryotes green-plants spermatopsida alive embryophytes}]
    [12 #{archaeplastida eukaryotes polypodiopsida green-plants alive embryophytes}]
    [13 #{archaeplastida eukaryotes green-plants spermatopsida alive conifers embryophytes}]
    [14 #{eukaryotes unikonts alive}]
    [15 #{eukaryotes unikonts alive opisthokonts}]
    [16 #{eukaryotes unikonts animals alive opisthokonts}]
    [17 #{eukaryotes fungi unikonts alive opisthokonts}]
    [18 #{eukaryotes amoebozoa unikonts alive}]])

(def collect-transitive-closure
  (f/collect-pure
   (fn [val incoming]
     (reduce into val incoming))))

(defn test-graph
  [types hierarchy]
  (let [g (f/compute-graph)
        vspec {::f/collect-fn collect-transitive-closure}
        verts (reduce
               (fn [acc v] (assoc acc v (f/add-vertex! g #{v} vspec)))
               {} types)]
    (doseq [[a b] hierarchy]
      (f/add-edge! g (verts a) (verts b) f/signal-forward nil))
    g))

(deftest test-transitive-closure
  (let [g (test-graph types hierarchy)]
    (is (= '([0 #{alive}] [1 #{eubacteria}] [2 #{eukaryotes}] [3 #{archaea}]
             [4 #{cyanobacteria}] [5 #{xenococcus}] [6 #{rivularia}] [7 #{archaeplastida}]
             [8 #{green-plants}] [9 #{rhodophyta}] [10 #{embryophytes}] [11 #{spermatopsida}]
             [12 #{polypodiopsida}] [13 #{conifers}] [14 #{unikonts}] [15 #{opisthokonts}]
             [16 #{animals}] [17 #{fungi}] [18 #{amoebozoa}])
           (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/scheduled-execution-context {:graph g}))]
      (prn :scheduler res)
      (is (= :converged (:type res)))
      (is (= expected-closure (fu/sorted-vertex-values (f/vertices g)))))))

(deftest test-transitive-closure-sync
  (let [g (test-graph types hierarchy)
        res (f/execute! (f/sync-execution-context {:graph g}))]
    (prn :sync res)
    (is (= :converged (:type res)))
    (is (= expected-closure (fu/sorted-vertex-values (f/vertices g))))))

(deftest ^:async test-transitive-closure-async
  (let [g (test-graph types hierarchy)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g :auto-stop true})))]
        (prn :async res)
        (is (= :converged (:type res)))
        (is (= expected-closure (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
