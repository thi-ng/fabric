(ns signalcollect.rdfs
  (:require
   [signalcollect.core :as sc]
   [clojure.test :refer :all]))

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
  (let [g (sc/graph)
        spec {:collect sc/collect-union}
        verts (reduce
               (fn [acc v] (assoc acc v (sc/add-vertex g (assoc spec :state #{v}))))
               {} types)]
    (doseq [[a b] hierarchy]
      (sc/edge (verts a) (verts b) {:signal sc/signal-forward}))
    g))

(deftest test-rdfs-simple
  (let [g (rdfs-test-graph types hierarchy)]
    (is (= '[[0 #{animal}] [1 #{vertebrae}] [2 #{mammal}]
             [3 #{human}] [4 #{dog}] [5 #{fish}] [6 #{shark}]]
           (sc/dump g)))
    (sc/execute-scored-sync g 1000 0 0)
    (is (= '[[0 #{animal}]
             [1 #{vertebrae animal}]
             [2 #{vertebrae mammal animal}]
             [3 #{vertebrae human mammal animal}]
             [4 #{vertebrae dog mammal animal}]
             [5 #{vertebrae fish animal}]
             [6 #{vertebrae shark fish animal}]]
           (sc/dump g)))))
