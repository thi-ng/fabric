(ns thi.ng.fabric.sssp
  #+cljs
  (:require-macros
   [cemerick.cljs.test :refer [is deftest with-test testing]])
  (:require
   [thi.ng.fabric.core :as f]
   [thi.ng.fabric.utils :as fu]
   #+clj  [clojure.test :refer :all]
   #+cljs [cemerick.cljs.test :as t]))

(defn signal-sssp
  [e v] (if (:state v) (+ (:weight e) (:state v))))

(defn collect-sssp
  [v sig]
  (if (:state v)
    (assoc v :state (min (:state v) sig))
    (assoc v :state sig)))

(defn score-sig-sssp
  [{:keys [state prev]}]
  (if (and state (or (not prev) (not= state prev))) 1 0))

;; a -> b -> c ; b -> d
(defn sssp-test-graph
  [g edges]
  (let [vspec {:collect collect-sssp :score-sig score-sig-sssp}
        espec {:signal signal-sssp :sig-map true}
        verts (reduce-kv
               (fn [acc k v] (assoc acc k (f/add-vertex g (assoc vspec :state v))))
               (sorted-map) (sorted-map 'a 0 'b nil 'c nil 'd nil 'e nil 'f nil))]
    (doseq [[a b w] edges]
      (f/edge (verts a) (verts b) (assoc espec :weight w)))
    g))

(defn make-strand
  [verts]
  (let [n (count verts)
        l (+ 1 (rand-int 3))
        s (rand-int (- n (* l 2)))]
    (reduce
     (fn [acc i]
       (let [v (+ (inc (peek acc)) (rand-int (/ (- n (peek acc)) 2)))]
         (if (< v n)
           (conj acc v)
           (reduced acc))))
     [s] (range l))))

(defn sssp-test-linked
  [g n ne]
  (let [vspec {:collect collect-sssp :score-sig score-sig-sssp}
        espec {:signal signal-sssp}
        verts (->> (range n)
                   (map (fn [_] (f/add-vertex g vspec)))
                   (cons (f/add-vertex g (assoc vspec :state 0)))
                   vec)]
    (dotimes [i ne]
      (->> (make-strand verts)
           (partition 2 1)
           (map (fn [[a b]] (f/edge (verts a) (verts b) espec)))
           (doall)))
    g))

(deftest test-sssp-simple
  (let [g (sssp-test-graph (f/graph) '[[a b] [b c] [c d] [a e] [d f] [e f]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/dump g)))
    (is (:converged (f/execute g {:iter 1000})))
    (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (fu/dump g)))))

(deftest test-sssp-weighted
  (let [g (sssp-test-graph (f/graph) '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/dump g)))
    (is (:converged (f/execute g {:iter 1000})))
    (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (fu/dump g)))))
