(ns thi.ng.fabric.test.vertexcolor
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

(defn rand-col-except
  [c numc]
  (loop [c' c]
    (if (== c' c)
      (recur (rand-int numc))
      c')))

(defn collect-color-vertex
  [numc]
  (fn [vertex]
    (let [neighbors (set (vals (::f/signal-map @(:state vertex))))]
      (if (neighbors @vertex)
        (f/update-value! vertex #(rand-col-except % numc))))))

(defn test-graph
  [numv numc prob]
  (let [g (f/compute-graph)
        vspec {::f/collect-fn (collect-color-vertex numc)}]
    (doseq [i (range numv)]
      (f/add-vertex! g (rand-int numc) vspec))
    (doseq [i (range numv) j (range numv)]
      (when (and (not= i j) (< (rand) prob))
        (let [vi (f/vertex-for-id g i)
              vj (f/vertex-for-id g j)]
          (f/add-edge! g vi vj f/signal-forward nil)
          (f/add-edge! g vj vi f/signal-forward nil)
          )))
    g))

(deftest test-vertex-coloring
  (let [g   (test-graph 100 15 0.05)
        _   (prn :graph-ready)
        res (f/execute! (f/sync-execution-context {:graph g :max-iter 5000}))]
    (prn :sync res)
    (prn (fu/sorted-vertex-values (f/vertices g)))
    (fu/vertices->dot "vcolor.dot" (f/vertices g) identity)
    (is (= :converged (:type res)))))

(deftest ^:async test-vertex-coloring-async
  (let [g (test-graph 100 15 0.05)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g :auto-stop true})))]
        (prn :async res)
        ;;(prn (fu/sorted-vertex-values (f/vertices g)))
        ;;(fu/vertices->dot "vcolor-async.dot" (f/vertices g) identity)
        (is (= :converged (:type res)))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
