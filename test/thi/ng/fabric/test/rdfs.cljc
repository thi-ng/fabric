(ns thi.ng.fabric.test.rdfs
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

(def types
  '[animal vertebrae mammal human dog fish shark])

(def hierarchy
  '[[animal vertebrae]
    [vertebrae mammal]
    [vertebrae fish]
    [fish shark]
    [mammal human]
    [mammal dog]])

(def rdfs-collect
  (f/collect-pure
   (fn [val incoming]
     (reduce into val incoming))))

(defn rdfs-test-graph
  [types hierarchy]
  (let [g (f/compute-graph)
        vspec {::f/collect-fn rdfs-collect}
        verts (reduce
               (fn [acc v] (assoc acc v (f/add-vertex! g #{v} vspec)))
               {} types)]
    (doseq [[a b] hierarchy]
      (f/add-edge! g (verts a) (verts b) f/signal-forward nil))
    g))

(deftest test-rdfs-simple
  (let [g (rdfs-test-graph types hierarchy)]
    (is (= '[[0 #{animal}] [1 #{vertebrae}] [2 #{mammal}]
             [3 #{human}] [4 #{dog}] [5 #{fish}] [6 #{shark}]]
           (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/sync-execution-context {:graph g}))]
      (prn res)
      (is (= :converged (:type res)))
      (is (= '[[0 #{animal}]
               [1 #{vertebrae animal}]
               [2 #{vertebrae mammal animal}]
               [3 #{vertebrae human mammal animal}]
               [4 #{vertebrae dog mammal animal}]
               [5 #{vertebrae fish animal}]
               [6 #{vertebrae shark fish animal}]]
             (fu/sorted-vertex-values (f/vertices g)))))))

(deftest ^:async test-rdfs-simple-async
  (let [g (rdfs-test-graph types hierarchy)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g :auto-stop true})))]
        (is (= :converged (:type res)))
        (is (= '[[0 #{animal}]
                 [1 #{vertebrae animal}]
                 [2 #{vertebrae mammal animal}]
                 [3 #{vertebrae human mammal animal}]
                 [4 #{vertebrae dog mammal animal}]
                 [5 #{vertebrae fish animal}]
                 [6 #{vertebrae shark fish animal}]]
               (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
