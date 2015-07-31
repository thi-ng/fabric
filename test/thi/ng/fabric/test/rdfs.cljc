(ns thi.ng.fabric.test.rdfs
  (:require
   [thi.ng.fabric.core :as f]
   [thi.ng.fabric.utils :as fu]
   #?(:clj
      [clojure.test :refer :all]
      :cljs
      [cemerick.cljs.test :refer-macros [is deftest with-test testing]])))

(def types
  '[animal vertebrae mammal human dog fish shark])

(def hierarchy
  '[[animal vertebrae]
    [vertebrae mammal]
    [vertebrae fish]
    [fish shark]
    [mammal human]
    [mammal dog]])

(defn rdfs-test-graph
  [types hierarchy]
  (let [g (f/graph)
        vspec {:collect f/collect-union}
        espec {:signal f/signal-forward}
        verts (reduce
               (fn [acc v] (assoc acc v (f/add-vertex g (assoc vspec :state #{v}))))
               {} types)]
    (doseq [[a b] hierarchy]
      (f/edge (verts a) (verts b) espec))
    g))

(deftest test-rdfs-simple
  (let [g (rdfs-test-graph types hierarchy)]
    (is (= '[[0 #{animal}] [1 #{vertebrae}] [2 #{mammal}]
             [3 #{human}] [4 #{dog}] [5 #{fish}] [6 #{shark}]]
           (fu/dump g)))
    (is (:converged (f/execute g {:iter 1000})))
    (is (= '[[0 #{animal}]
             [1 #{vertebrae animal}]
             [2 #{vertebrae mammal animal}]
             [3 #{vertebrae human mammal animal}]
             [4 #{vertebrae dog mammal animal}]
             [5 #{vertebrae fish animal}]
             [6 #{vertebrae shark fish animal}]]
           (fu/dump g)))))
