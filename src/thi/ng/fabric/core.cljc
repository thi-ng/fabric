(ns thi.ng.fabric.core
  #?@(:clj
      [(:require
        [thi.ng.xerror.core :as err]
        [taoensso.timbre :refer [debug info warn]]
        [clojure.core.reducers :as r]
        [clojure.core.async :refer [go go-loop chan close! <! >! alts! timeout]])]
      :cljs
      [(:require-macros
        [cljs.core.async.macros :refer [go go-loop]]
        [cljs-log.core :refer [debug info warn]])
       (:require
        [thi.ng.xerror.core :as err]
        [clojure.core.reducers :as r]
        [cljs.core.async :as a :refer [chan close! <! >! alts! timeout]])]))

;; #?(:clj (taoensso.timbre/set-level! :warn))

;; (warn :free-ram (.freeMemory (Runtime/getRuntime)))

(defprotocol IVertex
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-vertex! [_ v])
  (disconnect-all! [_])
  (close-input! [_])
  (input-channel [_])
  (connected-vertices [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_] [_ handler])
  (score-signal [_])
  (receive-signal [_ src sig])
  (set-value! [_ val])
  (update-value! [_ f]))

(defprotocol IComputeGraph
  (add-vertex! [_ val vspec])
  (remove-vertex! [_ v])
  (vertex-for-id [_ id])
  (vertices [_])
  (add-edge! [_ src dest f opts]))

(defprotocol IGraphExecutor
  (execute! [_])
  (stop! [_])
  (notify! [_ evt])
  (include-vertex! [_ v]))

(defn collect-pure
  [collect-fn]
  (fn [vertex]
    (swap! (:value vertex) #(collect-fn % (::uncollected @(:state vertex))))))

(def collect-into (collect-pure into))

(defn signal-forward
  [vertex _] @vertex)

(defn default-score-signal
  "Computes vertex signal score. Returns 0 if value equals prev-val,
  else returns 1."
  [vertex]
  (if (= @vertex @(:prev-val vertex)) 0 1))

(defn default-score-collect
  "Computes vertex collect score, here simply the number
  of ::uncollected signal values."
  [vertex] (-> vertex :state deref ::uncollected count))

(defn should-signal?
  [vertex thresh] (> (score-signal vertex) thresh))

(defn should-collect?
  [vertex thresh] (> (score-collect vertex) thresh))

(defn async-vertex-signal
  [vertex]
  (let [id   (:id vertex)
        outs @(:outs vertex)]
    (go-loop [outs outs]
      (let [[v [f opts]] (first outs)]
        (when v
          (let [signal (f vertex opts)]
            (if-not (nil? signal)
              (>! (input-channel v) [id signal])
              (warn "signal fn for" (:id v) "returned nil, skipping...")))
          (recur (next outs)))))))

(defn sync-vertex-signal
  [vertex]
  (let [id   (:id vertex)
        outs @(:outs vertex)]
    (loop [outs outs]
      (let [[v [f opts]] (first outs)]
        (when v
          (let [signal (f vertex opts)]
            (if-not (nil? signal)
              (receive-signal v id signal)
              (warn "signal fn for" (:id v) "returned nil, skipping...")))
          (recur (next outs)))))))

(def default-vertex-state
  {::uncollected      []
   ::signal-map       {}
   ::score-collect-fn default-score-collect
   ::score-signal-fn  default-score-signal
   ::collect-fn       collect-into
   ::signal-fn        async-vertex-signal
   ::input-channel-fn chan})

(defrecord Vertex [id value state prev-val in outs]
  #?@(:clj
       [clojure.lang.IDeref
        (deref
         [_] @value)]
       :cljs
       [IDeref
        (-deref
         [_] @value)])
  IVertex
  (set-value!
    [_ val] (reset! value val) #_(signal! _) _)
  (update-value!
    [_ f] (swap! value f) #_(signal! _) _)
  (collect!
    [_]
    ((::collect-fn @state) _)
    (swap! state assoc ::uncollected [])
    _)
  (score-collect
    [_] ((::score-collect-fn @state) _))
  (connect-to!
    [_ v sig-fn opts]
    (debug id "connecting to" (:id v) "(" (pr-str opts) ")")
    (swap! outs assoc v [sig-fn opts]) _)
  (connected-vertices
    [_] (keys @outs))
  (disconnect-vertex!
    [_ v]
    (debug id "disconnecting from" (:id v))
    (swap! outs dissoc v)
    _)
  (disconnect-all!
    [_] (run! #(disconnect-vertex! _ %) (keys @outs)) _)
  (close-input! [_]
    (if @in
      (do #_(debug id "closing...")
          (close! @in)
          (reset! in nil))
      (warn id "already closed"))
    _)
  (input-channel
    [_] (or @in (reset! in ((::input-channel-fn @state)))))
  (score-signal
    [_] ((::score-signal-fn @state) _))
  (signal!
    [_]
    (reset! prev-val @value)
    ((::signal-fn @state) _)
    _)
  (signal!
    [_ handler]
    (reset! prev-val @value)
    (handler _)
    _)
  (receive-signal
    [_ src sig]
    (let [sig-map (::signal-map @state)]
      (if-not (= sig (sig-map src))
        (swap! state
               #(-> %
                    (update ::uncollected conj sig)
                    (assoc-in [::signal-map src] sig)))
        (info (str id " ignoring unchanged signal: " (pr-str sig)))))
    _))

#?(:clj
   (defmethod clojure.pprint/simple-dispatch Vertex
     [^Vertex o] ((get-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap) o)))
#?(:clj
   (defmethod print-method Vertex
     [^Vertex o ^java.io.Writer w] (.write w (.toString (into {} o)))))

(defn vertex
  [id val state]
  (map->Vertex
   {:id       id
    :value    (atom val)
    :state    (atom (merge default-vertex-state state))
    :prev-val (atom nil)
    :in       (atom nil)
    :outs     (atom {})}))

(defrecord InMemoryGraph [state]
  IComputeGraph
  (add-vertex!
    [_ val vspec]
    (-> state
        (swap!
         (fn [state]
           (let [id (:next-id state)
                 v  (vertex id val vspec)]
             (-> state
                 (update :next-id inc)
                 (update :vertices assoc id v)
                 (assoc  :curr-vertex v)))))
        :curr-vertex))
  (remove-vertex!
    [_ v]
    (when (get-in @state [:vertices (:id v)])
      (swap! state update :vertices dissoc (:id v))
      (disconnect-all! v)
      true))
  (vertex-for-id
    [_ id] (get-in @state [:vertices id]))
  (vertices
    [_] (-> @state :vertices vals))
  (add-edge!
    [_ src dest sig-fn opts]
    (connect-to! src dest sig-fn opts)))

(defrecord LoggedGraph [g log-chan]
  IComputeGraph
  (add-vertex!
    [_ val vspec]
    (let [v (add-vertex! g val vspec)]
      (go (>! log-chan [:add-vertex (:id v) val]))
      v))
  (remove-vertex! [_ v]
    (when (remove-vertex! g v)
      (go (>! log-chan [:remove-vertex (:id v) @v]))
      true))
  (vertex-for-id
    [_ id] (vertex-for-id g id))
  (vertices
    [_] (vertices g))
  (add-edge!
    [_ src dest f opts]
    (connect-to! src dest f opts)
    (go (>! log-chan [:add-edge (:id src) (:id dest) f opts]))))

(defn compute-graph
  [] (InMemoryGraph. (atom {:vertices {} :next-id 0})))

(defn logged-compute-graph
  ([log-chan] (LoggedGraph. (compute-graph) log-chan))
  ([g log-chan] (LoggedGraph. g log-chan)))

(defn eager-async-vertex-processor
  [{:keys [id] :as vertex} ctx]
  (let [{:keys [collect-thresh signal-thresh]} @ctx
        in (input-channel vertex)]
    (go-loop []
      (let [[src-id sig] (<! in)]
        (if sig
          (do (debug id "receive from" src-id ":" (pr-str sig))
              (receive-signal vertex src-id sig)
              (when (should-collect? vertex collect-thresh)
                (notify! ctx [:collect id])
                (collect! vertex)
                ;;(debug id "post-collection:" (pr-str @vertex))
                (when (should-signal? vertex signal-thresh)
                  (notify! ctx [:signal id])
                  (signal! vertex async-vertex-signal)))
              (recur))
          ;;(debug id " stopped")
          )))
    vertex))

(defn- now [] #?(:clj (System/nanoTime) :cljs (.getTime (js/Date.))))

(defn execution-result
  [type colls sigs t0 & [opts]]
  (->> {:collections colls
        :signals     sigs
        :type        type
        :runtime     #?(:clj (* (- (now) t0) 1e-6) :cljs (- (now) t0))}
       (merge opts)))

(defn async-execution-result
  [type out colls sigs t0 & [opts]]
  (go (>! out (execution-result type colls sigs t0 opts))))

(defn stop-async-execution
  [bus vertices]
  (info "stopping execution context...")
  (run! close! bus)
  (run! close-input! vertices))

(def default-async-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :processor      eager-async-vertex-processor
   :bus-size       16
   :timeout        25
   :max-ops        1e6})

(defn async-execution-context
  [opts]
  (let [ctx       (merge default-async-context-opts opts)
        bus       (vec (repeatedly (:bus-size ctx) chan))
        ctx       (assoc ctx :bus bus :result (or (:result ctx) (chan)))
        processor (:processor ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        max-t     (:timeout ctx)
        result    (:result ctx)]
    (reify
      #?@(:clj
           [clojure.lang.IDeref
            (deref [_] ctx)]
           :cljs
           [IDeref
            (-deref [_] ctx)])
      IGraphExecutor
      (stop!
        [_]
        (stop-async-execution bus (vertices (:graph ctx)))
        result)
      (notify! [_ evt]
        (go (>! (rand-nth bus) evt)))
      (include-vertex!
        [_ v]
        (processor v _)
        (when (should-collect? v c-thresh)
          (collect! v))
        (when (should-signal? v s-thresh)
          (signal! v async-vertex-signal))
        _)
      (execute!
        [_]
        (let [t0 (now)]
          (go
            (run! #(include-vertex! _ %) (vertices (:graph ctx)))
            (loop [colls 0, sigs 0]
              (if (<= (+ colls sigs) max-ops)
                (let [t (if max-t (timeout max-t))
                      [[evt v ex] port] (alts! (if max-t (conj bus t) bus))]
                  (if (= port t)
                    (if (:auto-stop ctx)
                      (do (stop! _)
                          (async-execution-result :converged result colls sigs t0))
                      (do (async-execution-result :converged result colls sigs t0)
                          (recur colls sigs)))
                    (case evt
                      :collect (recur (inc colls) sigs)
                      :signal  (recur colls (inc sigs))
                      :error   (do (warn ex "@ vertex" v)
                                   (when (:auto-stop ctx) (stop! _))
                                   (async-execution-result
                                    :error result colls sigs t0
                                    {:reason-event     evt
                                     :reason-exception ex
                                     :reason-vertex    v}))
                      (do (warn "execution interrupted")
                          (async-execution-result
                           :stopped result colls sigs t0)))))
                (async-execution-result :max-ops-reached result colls sigs t0))))
          result)))))

(defn sync-signal-vertices
  [vertices thresh]
  (loop [sigs 0, verts vertices]
    (if verts
      (let [v (first verts)]
        (if (should-signal? v thresh)
          (do (debug (:id v) "signaling")
              (signal! v sync-vertex-signal)
              (recur (inc sigs) (next verts)))
          (recur sigs (next verts))))
      sigs)))

(defn sync-collect-vertices
  [vertices thresh]
  (loop [colls 0, verts vertices]
    (if verts
      (let [v (first verts)]
        (if (should-collect? v thresh)
          (do (debug (:id v) "collecting")
              (collect! v)
              (recur (inc colls) (next verts)))
          (recur colls (next verts))))
      colls)))

(defn parallel-sync-signal-vertices
  [vertices thresh]
  (r/fold
   +
   (fn
     ([] 0)
     ([acc v] (if (should-signal? v thresh) (do (signal! v sync-vertex-signal) (inc acc)) acc)))
   vertices))

(defn parallel-sync-collect-vertices
  [vertices thresh]
  (r/fold
   +
   (fn
     ([] 0)
     ([acc v] (if (should-collect? v thresh) (do (collect! v) (inc acc)) acc)))
   vertices))

(def default-sync-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :max-iter       1e6
   :signal-fn      sync-signal-vertices
   :collect-fn     sync-collect-vertices})

(defn sync-execution-context
  [opts]
  (let [ctx       (merge default-sync-context-opts opts)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        coll-fn   (:collect-fn ctx)
        sig-fn    (:signal-fn ctx)
        max-iter  (:max-iter ctx)]
    (reify
      #?@(:clj
           [clojure.lang.IDeref
            (deref [_] ctx)]
           :cljs
           [IDeref
            (-deref [_] ctx)])
      IGraphExecutor
      (stop! [_] (err/unsupported!))
      (notify! [_ evt] (err/unsupported!))
      (include-vertex! [_ v] (err/unsupported!))
      (execute!
        [_]
        (let [t0 (now)]
          (loop [i 0, colls 0, sigs 0]
            (if (<= i max-iter)
              (let [verts (vertices (:graph ctx))
                    sigs' (sig-fn verts s-thresh)
                    colls' (coll-fn verts c-thresh)]
                (if (and (pos? sigs') (pos? colls'))
                  (recur (inc i) (long (+ colls colls')) (long (+ sigs sigs')))
                  (execution-result :converged colls sigs t0 {:iterations i})))
              (execution-result :max-iter-reached colls sigs t0 {:iterations i}))))))))
