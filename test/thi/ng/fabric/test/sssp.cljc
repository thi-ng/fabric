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
   (fn [val incoming]
     (if val (reduce min val incoming) (reduce min incoming)))))

(defn sssp-test-graph
  [edges]
  (let [g     (f/compute-graph)
        vspec {::f/collect-fn collect-sssp}
        verts (reduce-kv
               (fn [acc k v] (assoc acc k (f/add-vertex! g v vspec)))
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

(defn random-graph-spec
  [num-verts num-edges e-length]
  (let [verts (vec (range (inc num-verts)))]
    {:num-verts num-verts
     :edges (mapcat
             (fn [_] (->> (make-strand verts e-length) (partition 2 1)))
             (range num-edges))}))

(defn random-graph-from-spec
  [{:keys [num-verts edges]}]
  (let [g     (f/compute-graph)
        vspec {::f/collect-fn collect-sssp}
        verts (->> (range num-verts)
                   (map (fn [_] (f/add-vertex! g nil vspec)))
                   (cons (f/add-vertex! g 0 vspec))
                   vec)]
    (doseq [[a b] edges]
      (f/add-edge! g (verts a) (verts b) signal-sssp 1))
    g))

;; a -> b -> c ; b -> d

(deftest test-sssp-simple
  (let [g (sssp-test-graph '[[a b] [b c] [c d] [a e] [d f] [e f]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/scheduled-execution-context {:graph g}))]
      (is (= :converged (:type res)))
      (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (fu/sorted-vertex-values (f/vertices g)))))))

(deftest test-sssp-simple-sync
  (let [g (sssp-test-graph '[[a b] [b c] [c d] [a e] [d f] [e f]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/sync-execution-context {:graph g}))]
      (is (= :converged (:type res)))
      (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (fu/sorted-vertex-values (f/vertices g)))))))

(deftest ^:async test-sssp-simple-async
  (let [g (sssp-test-graph '[[a b] [b c] [c d] [a e] [d f] [e f]])
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g :auto-stop true})))]
        (is (= :converged (:type res)))
        (is (= [[0 0] [1 1] [2 2] [3 3] [4 1] [5 2]] (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))

(deftest test-sssp-weighted
  (let [g (sssp-test-graph '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/scheduled-execution-context {:graph g}))]
      (prn :weighted-scheduler res)
      (is (= :converged (:type res)))
      (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (fu/sorted-vertex-values (f/vertices g)))))))

(deftest test-sssp-weighted-sync
  (let [g (sssp-test-graph '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])]
    (is (= [[0 0] [1 nil] [2 nil] [3 nil] [4 nil] [5 nil]] (fu/sorted-vertex-values (f/vertices g))))
    (let [res (f/execute! (f/sync-execution-context {:graph g}))]
      (prn :weighted-sync res)
      (is (= :converged (:type res)))
      (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (fu/sorted-vertex-values (f/vertices g)))))))

(deftest ^:async test-sssp-weighted-async
  (let [g (sssp-test-graph '[[a b 1] [b c 10] [c d 2] [a e 4] [d f 7] [e f 100]])
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g :auto-stop true})))]
        (prn :weighted-async res)
        (is (= :converged (:type res)))
        (is (= [[0 0] [1 1] [2 11] [3 13] [4 4] [5 20]] (fu/sorted-vertex-values (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))

(def spec (random-graph-spec 10000 30000 3))

(deftest test-sssp-random-two-pass
  (let [g (random-graph-from-spec spec)]
    (let [res (f/execute! (f/scheduled-execution-context
                           {:graph g
                            :processor f/parallel-two-pass-processor
                            :scheduler f/two-pass-scheduler}))]
      (prn :random-two-pass res)
      (is (= :converged (:type res)))
      (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g)))))))

(deftest test-sssp-random-prob
  (let [g (random-graph-from-spec spec)]
    (let [res (f/execute! (f/scheduled-execution-context
                           {:graph g
                            :processor f/probabilistic-single-pass-processor
                            :scheduler f/single-pass-scheduler}))]
      (prn :random-prob res)
      (is (= :converged (:type res)))
      (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g)))))))

(deftest test-sssp-random-eager
  (let [g (random-graph-from-spec spec)]
    (let [res (f/execute! (f/scheduled-execution-context
                           {:graph g
                            :processor f/eager-probabilistic-single-pass-processor
                            :scheduler f/single-pass-scheduler}))]
      (prn :random-eager res)
      (is (= :converged (:type res)))
      (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g)))))))

(deftest test-sssp-random-sync
  (let [g (random-graph-from-spec spec)]
    (let [res (f/execute! (f/sync-execution-context
                           {:graph g
                            :max-iter 10000}))]
      (prn :random-sync res)
      (is (= :converged (:type res)))
      (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g)))))))

(deftest ^:async test-sssp-random-async
  (let [g (random-graph-from-spec spec)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context
                                 {:graph g :auto-stop true})))]
        (prn :random-async res)
        (is (= :converged (:type res)))
        (is (< 5 (transduce (comp (map deref) (filter identity)) max 0 (f/vertices g))))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
