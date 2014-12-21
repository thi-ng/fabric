(ns thi.ng.fabric.rdfs
  #+cljs
  (:require-macros
   [cemerick.cljs.test :refer [is deftest with-test testing]])
  (:require
   [thi.ng.fabric.core :as f]
   #+clj  [clojure.test :refer :all]
   #+cljs [cemerick.cljs.test :as t]))

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
        spec {:collect f/collect-union}
        verts (reduce
               (fn [acc v] (assoc acc v (f/add-vertex g (assoc spec :state #{v}))))
               {} types)]
    (doseq [[a b] hierarchy]
      (f/edge (verts a) (verts b) {:signal f/signal-forward}))
    g))

(deftest test-rdfs-simple
  (let [g (rdfs-test-graph types hierarchy)]
    (is (= '[[0 #{animal}] [1 #{vertebrae}] [2 #{mammal}]
             [3 #{human}] [4 #{dog}] [5 #{fish}] [6 #{shark}]]
           (f/dump g)))
    (f/execute g {:iter 1000})
    (is (= '[[0 #{animal}]
             [1 #{vertebrae animal}]
             [2 #{vertebrae mammal animal}]
             [3 #{vertebrae human mammal animal}]
             [4 #{vertebrae dog mammal animal}]
             [5 #{vertebrae fish animal}]
             [6 #{vertebrae shark fish animal}]]
           (f/dump g)))))
