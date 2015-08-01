(ns thi.ng.fabric.async
  (:require
   [clojure.core.async :as a :refer [go go-loop chan close! <! >! alts! timeout]]
   [taoensso.timbre :refer [info warn error]]))

(defprotocol IVertex
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-from! [_ v])
  (disconnect! [_])
  (input-channel [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_])
  (score-signal [_])
  (receive-signal [_ src sig]))

(defprotocol IComputeGraph
  (add-vertex! [_ vstate])
  (vertex-for-id [_ id])
  (vertices [_]))

(defprotocol IGraphExecutor
  (execute! [_ vertices]))

(defn default-collect
  [vertex]
  (swap! (:state vertex) #(update % ::val into (::uncollected %))))

(defn signal-forward
  [state _] (::val state))

(defn default-score-signal
  "Computes vertex signal score. Returns 0 if state's ::val
  equals :last-sig-state, else returns 1."
  [{:keys [state last-sig-state]}]
  (if (= (::val @state) @last-sig-state) 0 1))

(defn default-score-collect
  "Computes vertex collect score, here simply the number
  of ::uncollected signal values."
  [vertex] (-> vertex :state deref ::uncollected count))

(defn should-signal?
  [vertex thresh] (> (score-signal vertex) thresh))

(defn should-collect?
  [vertex thresh] (> (score-collect vertex) thresh))

(defrecord Vertex [id state last-sig-state in outs]
  clojure.lang.IDeref
  (deref [_] (::val @state))
  IVertex
  (collect! [_] ((::collect-fn @state) _))
  (score-collect [_] ((::score-collect-fn @state) _))
  (connect-to!
    [_ v sig-fn opts]
    (info (format "%d connecting to %d (%s)" id (:id v) (pr-str opts)))
    (swap! outs assoc v [sig-fn opts]) _)
  (disconnect-from!
    [_ v]
    (info (format "%d disconnecting from %d" id (:id v)))
    (swap! outs dissoc v)
    _)
  (disconnect! [_]
    (info (format "%d disconnecting..." id))
    (close! in)
    _)
  (input-channel
    [_] in)
  (score-signal [_] ((::score-signal-fn @state) _))
  (signal!
    [_]
    (let [state @state]
      (reset! last-sig-state (::val state))
      (go-loop [outs @outs]
        (let [[v [f opts]] (first outs)]
          (when v
            (let [signal (f state opts)]
              (if-not (nil? signal)
                (>! (input-channel v) [id signal])
                (warn (format "signal fn for %d return nil, skipping..." (:id v)))))
            (recur (next outs)))))
      _))
  (receive-signal
    [_ src sig]
    (swap! state
           (fn [state]
             (-> state
                 (update ::uncollected conj sig)
                 (assoc-in [::signal-map src] sig))))
    _))

#?(:clj
   (defmethod clojure.pprint/simple-dispatch Vertex
     [^Vertex o] ((get-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap) o)))
#?(:clj
   (defmethod print-method Vertex
     [^Vertex o ^java.io.Writer w] (.write w (.toString (into {} o)))))

(def default-vertex-state
  {::uncollected      []
   ::signal-map       {}
   ::score-collect-fn default-score-collect
   ::score-signal-fn  default-score-signal
   ::collect-fn       default-collect})

(def default-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :timeout        100
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
  (vertex-for-id
    [_ id] (-> @state :vertices id))
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
          (do (info id "receive from" src-id sig)
              (receive-signal vertex src-id sig)
              (when (should-collect? vertex collect-thresh)
                (when (>! ctrl [:collect id])
                  (collect! vertex)
                  (swap! state assoc ::uncollected [])
                  (when (should-signal? vertex signal-thresh)
                    (>! ctrl [:signal id])
                    (signal! vertex))))
              (recur))
          (info id "stopped"))))
    vertex))

(defn execution-result
  [p t0 colls sigs]
  (deliver
   p {:collections colls
      :signals     sigs
      :time        (* (- (System/nanoTime) t0) 1e-6)}))

(defn async-context
  [opts]
  (let [ctrl     (chan)
        ctx      (merge default-context-opts {:ctrl ctrl} opts)
        c-thresh (:collect-thresh ctx)
        s-thresh (:signal-thresh ctx)
        max-ops  (:max-ops ctx)
        max-t    (:timeout ctx)]
    (reify
      clojure.lang.IDeref
      (deref [_] ctx)
      IGraphExecutor
      (execute! [_ g]
        (let [t0  (System/nanoTime)
              res (promise)]
          (go
            (loop [verts (vertices g)]
              (when-let [v (first verts)]
                (when (should-collect? v c-thresh)
                  ((-> v :state deref ::collect-fn) vertex))
                (when (should-signal? v s-thresh)
                  (signal! v))
                (recur (next verts))))
            (loop [colls 0 sigs 0]
              (if (<= (+ colls sigs) max-ops)
                (let [t (timeout max-t)
                      [[evt v ex] port] (alts! [ctrl t])]
                  (if (= port t)
                    (do (info "graph converged, done...")
                        (execution-result res t0 colls sigs))
                    (case evt
                      :collect (recur (inc colls) sigs)
                      :signal  (recur colls (inc sigs))
                      :error   (warn ex "@ vertex" v)
                      (do (info "wrong event" evt v)))))
                (do (info "reached max ops: " max-ops)
                    (execution-result res t0 colls sigs)))))
          res)))))

(def g (compute-graph))
(def ctx (async-context {}))

(defn signal-sssp
  [state dist] (if-let [v (::val state)] (+ dist v)))

(defn collect-sssp
  [vertex]
  (swap! (:state vertex)
         (fn [{:keys [val] :as state}]
           (update state ::val
                   (if val
                     (fn [_] (reduce min _ (::uncollected state)))
                     (fn [_] (reduce min (::uncollected state))))))))

(def a (eager-async-vertex (add-vertex! g {::val 0   ::collect-fn collect-sssp}) ctx))
(def b (eager-async-vertex (add-vertex! g {::val nil ::collect-fn collect-sssp}) ctx))
(def c (eager-async-vertex (add-vertex! g {::val nil ::collect-fn collect-sssp}) ctx))
(def d (eager-async-vertex (add-vertex! g {::val nil ::collect-fn collect-sssp}) ctx))

(connect-to! a b signal-sssp 1)
(connect-to! a c signal-sssp 3)
(connect-to! b c signal-sssp 1)
(connect-to! c d signal-sssp 1)

(prn @(execute! ctx g))

(prn @a)
(prn @b)
(prn @c)
(prn @d)


(comment
  (disconnect! a)
  (disconnect! b)
  (disconnect! c)
  (disconnect! d)
  )
