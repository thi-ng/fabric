(ns thi.ng.fabric.test.sssp
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

(defn signal-sssp
  [vertex dist] (if-let [v @vertex] (+ dist v)))

(def collect-sssp
  (f/collect-pure
   (fn [val uncollected]
     (if val (reduce min val uncollected) (reduce min uncollected)))))

;; a -> b -> c ; b -> d
(defn sssp-test-graph
  [g edges]
  (let [vspec {::f/collect-fn collect-sssp}
        verts (reduce-kv
               (fn [acc k v] (assoc acc k (f/add-vertex! g (assoc vspec :val v))))
               {} {'a 0 'b nil 'c nil 'd nil 'e nil 'f nil})]
    (doseq [[a b w] edges]
      (f/add-edge! g (verts a) (verts b) signal-sssp (or w 1)))
    g))

(defn make-strand
  [verts e-length]
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

(defn sssp-random-graph
  [g num-verts num-edges e-length]
  (let [vspec {::f/collect-fn collect-sssp}
        verts (->> (range num-verts)
                   (map (fn [_] (f/add-vertex! g vspec)))
                   (cons (f/add-vertex! g (assoc vspec :val 0)))
                   vec)]
    (dotimes [i num-edges]
      (->> (make-strand verts e-length)
           (partition 2 1)
           (map (fn [[a b]] (f/add-edge! g (verts a) (verts b) signal-sssp 1)))
           (doall)))
    g))

(deftest ^:async test-sssp-simple
  (let [g (sssp-test-graph (f/compute-graph) '[[a b] [b c] [c d] [a e] [d f] [e f]])
        notify (chan)]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (go
      (let [res (<! (f/execute! (f/execution-context {:graph g})))]
        (is (= :converged (:type res)))
        (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))

(deftest ^:async test-sssp-weighted
  (let [g (sssp-test-graph (f/compute-graph) '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])
        notify (chan)]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (go
      (let [res (<! (f/execute! (f/execution-context {:graph g})))]
        (is (= :converged (:type res)))
        (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))

(deftest ^:async test-sssp-random
  (let [g (sssp-random-graph (f/compute-graph) 1000 2000 3)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/execution-context {:graph g :bus-size 64 :timeout 100})))]
        (prn res)
        (is (= :converged (:type res)))
        (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
