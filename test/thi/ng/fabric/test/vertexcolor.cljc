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

(def colors
  ["#ff0000" "#00ff00" "#0000ff" "#ffff00" "#00ffff" "#ff00ff"
   "#c00000" "#00c000" "#0000c0" "#c0c000" "#00c0c0" "#c000c0"
   "#800000" "#008000" "#000080" "#808000" "#008080" "#800080"])

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

(defn graph-spec
  [numv numc prob]
  {:colors   numc
   :vertices (repeatedly numv #(rand-int numc))
   :edges    (for [a (range numv)
                   b (range numv)
                   :when (and (not= a b) (< (rand) prob))]
               [a b])})

(defn graph-from-spec
  [{:keys [colors vertices edges]}]
  (let [g (f/compute-graph)
        vspec {::f/collect-fn (collect-color-vertex colors)}]
    (doseq [v vertices]
      (f/add-vertex! g v vspec))
    (doseq [[a b] edges
            :let  [va (f/vertex-for-id g a)
                   vb (f/vertex-for-id g b)]]
      (f/add-edge! g va vb f/signal-forward nil)
      (f/add-edge! g vb va f/signal-forward nil))
    g))

(defn export-graph
  [path g]
  (fu/vertices->dot
   path (f/vertices g) identity
   (fn [v val]
     (format "%d[label=\"%d (%s)\",color=\"%s\"];\n"
             (:id v) (:id v) val (colors @v)))))

(defn valid-vertex?
  [v] (let [val @v] (every? #(not= val @%) (f/connected-vertices v))))

(defn valid-graph?
  [g] (every? valid-vertex? (f/vertices g)))

(def spec (graph-spec 1000 180 0.05))

(deftest test-vertex-coloring
  (let [g   (graph-from-spec spec)
        res (f/execute! (f/scheduled-execution-context {:graph g}))]
    (prn :scheduler res)
    ;;(prn (fu/sorted-vertex-values (f/vertices g)))
    ;;(export-graph "vcolor.dot" g)
    (is (= :converged (:type res)))
    (is (valid-graph? g))))

(deftest test-vertex-coloring-sync
  (let [g   (graph-from-spec spec)
        res (f/execute! (f/sync-execution-context {:graph g :max-iter 5000}))]
    (prn :sync res)
    (is (= :converged (:type res)))
    (is (valid-graph? g))))

(deftest ^:async test-vertex-coloring-async
  (let [g (graph-from-spec spec)
        notify (chan)]
    (go
      (let [res (<! (f/execute! (f/async-execution-context {:graph g})))]
        (prn :async res)
        (is (= :converged (:type res)))
        (is (valid-graph? g))
        (>! notify :ok)))
    #?(:clj (<!! notify) :cljs (take! notify (fn [_] (done))))))
