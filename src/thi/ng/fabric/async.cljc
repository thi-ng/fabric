(ns thi.ng.fabric.async
  (:require
   [clojure.core.async :as a :refer [go go-loop chan close! <! >! alts! timeout]]
   [taoensso.timbre :refer [info warn error]]))

(defprotocol IVertex
  (connect-to! [_ v sig-fn])
  (disconnect-from! [_ v])
  (disconnect! [_])
  (input-channel [_])
  (signal! [_])
  (receive-signal [_ src sig]))

(defprotocol IComputeGraph
  (add-vertex! [_ vstate])
  (vertices [_]))

(defprotocol IGraphExecutor
  (execute! [_ vertices]))

(defn default-collect
  [vertex]
  (swap! (:state vertex) #(update % :val into (:uncollected %))))

(defn signal-forward [state] (:val state))

(defn default-score-signal
  [{:keys [state last-sig-state] :as vertex}]
  (if (= (:val @state) @last-sig-state) 0 1))

(defn default-score-collect
  [vertex] (-> vertex :state deref :uncollected count))

(defn should-signal?
  [vertex thresh]
  (> ((-> vertex :state deref :score-signal) vertex) thresh))

(defn should-collect?
  [vertex thresh]
  (> ((-> vertex :state deref :score-collect) vertex) thresh))

(defrecord Vertex [id state last-sig-state in outs]
  IVertex
  (connect-to!
    [_ v sig-fn]
    (swap! outs assoc v sig-fn) _)
  (disconnect-from!
    [_ v] (swap! outs dissoc v) _)
  (disconnect! [_]
    (close! in) _)
  (input-channel
    [_] in)
  (signal!
    [_]
    (let [state @state]
      (reset! last-sig-state (:val state))
      (go-loop [outs @outs]
        (let [[v f] (first outs)]
          (when v
            (>! (input-channel v) [id (f state)])
            (recur (next outs)))))
      _))
  (receive-signal
    [_ src sig]
    (swap! state
           (fn [state]
             (-> state
                 (update :uncollected conj sig)
                 (assoc-in [:signal-map src] sig))))
    _))

(def default-vertex-state
  {:uncollected       []
   :signal-map        {}
   :score-collect     default-score-collect
   :score-signal      default-score-signal
   :collect-fn        default-collect})

(def default-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :collections    0
   :signals        0
   :timeout        1000
   :max-ops        1e6})

(defn vertex
  [id state]
  (map->Vertex
   {:id             id
    :state          (atom (merge default-vertex-state state))
    :last-sig-state (atom nil)
    :in             (chan)
    :outs           (atom {})}))

(defrecord Graph [state]
  IComputeGraph
  (add-vertex!
    [_ vstate]
    (-> state
        (swap!
         (fn [state]
           (let [id (:next-id state)
                 v  (vertex id vstate)]
             (-> state
                 (update :next-id inc)
                 (update :vertices assoc id v)
                 (assoc  :curr-vertex v)))))
        :curr-vertex))
  (vertices
    [_] (-> @state :vertices vals)))

(defn compute-graph
  [] (Graph. (atom {:vertices {} :next-id 0})))

(defn eager-async-vertex
  [{:keys [id in state] :as vertex} ctx]
  (let [{:keys [collect-thresh signal-thresh ctrl]} @ctx]
    (go-loop []
      (let [[src-id sig] (<! in)]
        (if sig
          (do (info id "receive from " src-id sig)
              (receive-signal vertex src-id sig)
              (when (should-collect? vertex collect-thresh)
                (when (>! ctrl [:collect id])
                  ((:collect-fn @state) vertex)
                  (swap! state assoc :uncollected [])
                  (when (should-signal? vertex signal-thresh)
                    (>! ctrl [:signal id])
                    (signal! vertex))))
              (recur))
          (info id "disconnected"))))
    vertex))

(defn async-context
  [opts]
  (let [ctrl     (chan)
        ctx      (merge default-context-opts {:ctrl ctrl} opts)
        c-thresh (:collect-thresh ctx)
        s-thresh (:signal-thresh ctx)
        ctx      (atom ctx)]
    (reify
      clojure.lang.IDeref
      (deref [_] @ctx)
      IGraphExecutor
      (execute! [_ vertices]
        (go
          (loop [verts vertices]
            (when-let [v (first verts)]
              (when (should-signal? v s-thresh)
                (signal! v))
              (recur (next verts))))
          (loop []
            (if (<= (+ (:collections @ctx) (:signals @ctx)) (:max-ops @ctx))
              (let [t (timeout (:timeout @ctx))
                    [[evt v ex] port] (alts! [ctrl t])]
                (if (= port t)
                  (info "graph converged, done...")
                  (case evt
                    :collect (do (swap! ctx update :collections inc) (recur))
                    :signal  (do (swap! ctx update :signals inc) (recur))
                    :error   (warn ex "@ vertex" v)
                    (info "done" (:collections @ctx) (:signals @ctx)))))
              (info "reached max ops: " (:max-ops @ctx)))))))))

(def g (compute-graph))
(def ctx (async-context {}))

(def a (eager-async-vertex (add-vertex! g {:val 'a :collect-fn default-collect}) ctx))
(def b (eager-async-vertex (add-vertex! g {:val '[b] :collect-fn default-collect}) ctx))
(def c (eager-async-vertex (add-vertex! g {:val '[c] :collect-fn default-collect}) ctx))

(connect-to! a b signal-forward)
(connect-to! a c signal-forward)
(connect-to! b c signal-forward)

(execute! ctx (vertices g))

(comment
  (disconnect! a)
  (disconnect! b)
  (disconnect! c)
  )
