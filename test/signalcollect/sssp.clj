(ns signalcollect.sssp
  (:require
   [clojure.test :refer :all]
   [signalcollect.core :as sc]))

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
  [edges]
  (let [g (sc/graph)
        spec {:collect collect-sssp :score-sig score-sig-sssp}
        verts (reduce-kv
               (fn [acc k v] (assoc acc k (sc/add-vertex g (assoc spec :state v))))
               (sorted-map) (sorted-map 'a 0 'b nil 'c nil 'd nil 'e nil 'f nil))]
    (doseq [[a b w] edges]
      (sc/edge (verts a) (verts b) {:weight w :signal signal-sssp :sig-map true}))
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
  [n ne]
  (let [g (sc/graph)
        spec {:collect collect-sssp :score-sig score-sig-sssp}
        verts (->> (range n)
                   (map (fn [_] (sc/add-vertex g spec)))
                   (cons (sc/add-vertex g (assoc spec :state 0)))
                   vec)]
    (dotimes [i ne]
      (->> (make-strand verts)
           (partition 2 1)
           (map (fn [[a b]] (sc/edge (verts a) (verts b) {:signal signal-sssp})))
           (doall)))
    g))

(deftest test-sssp-simple
  (let [g (sssp-test-graph '[[a b] [b c] [c d] [a e] [d f] [e f]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (sc/dump g)))
    (sc/execute-scored-sync g 1000 0 0)
    (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (sc/dump g)))))

(deftest test-sssp-weighted
  (let [g (sssp-test-graph '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (sc/dump g)))
    (sc/execute-scored-sync g 1000 0 0)
    (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (sc/dump g)))))
