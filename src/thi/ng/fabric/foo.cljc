(ns thi.ng.fabric.foo)

(defprotocol PComputeGraph
  (vertex-for-id [_ id])
  (add-vertex [_ v])
  (add-edge [_ a b])
  (execute [_ spec]))

(defprotocol PTripleGraph
  (add-triple [_ t]))

(defprotocol PEdge
  (connect-to [_ to type]))

(defrecord TripleVertex
    [id triple state])

(defn triple-vertex
  [id triple]
  (TripleVertex.
   id triple
   (atom {:out {}})))

(defrecord IndexVertex
    [id pos state]
  PEdge
  (connect-to [_ dest f] (swap! state update-in [:out f] (fnil conj #{}) dest) _))

(defn index-vertex
  [id pos]
  (IndexVertex.
   id pos
   (atom {:out {}})))

(deftype Graph
    [state]
  clojure.lang.IDeref
  (deref
    [_] @state)
  Object
  (toString [_]
    (pr-str (:vertices @state)))
  PComputeGraph
  (vertex-for-id
    [_ id] (get-in @state [:vertices id]))
  (add-vertex
    [_ vctor]
    (dosync
     (let [id (:next-id @state)
           v  (vctor id)]
       (alter state #(-> % (update :next-id inc) (update :vertices assoc id v)))
       v)))
  (execute [_ spec]))


(defrecord TripleGraph
    [g subjects preds objects]
  PTripleGraph
  (add-triple
    [_ [s p o :as t]]
    (let [sv (or (subjects s) (add-vertex g #(index-vertex % 0)))
          pv (or (preds p) (add-vertex g #(index-vertex % 1)))
          ov (or (objects o) (add-vertex g #(index-vertex % 2)))
          v  (add-vertex g #(triple-vertex % t))]
      (connect-to sv v :triple)
      (connect-to pv v :triple)
      (connect-to ov v :triple)
      (TripleGraph.
       g
       (assoc subjects s sv)
       (assoc preds p pv)
       (assoc objects o ov)))))

(defn graph
  []
  (Graph. (ref {:next-id 0 :vertices {}})))

(defn triple-graph
  []
  (TripleGraph. (graph) {} {} {}))

(def triples '[[karsten nick toxi] [toxi author fabric] [toxi author trio]])

(def g (reduce add-triple (triple-graph) triples))
