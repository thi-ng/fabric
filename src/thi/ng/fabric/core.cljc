(ns thi.ng.fabric.core
  #?@(:clj
      [(:require
        [thi.ng.xerror.core :as err]
        [taoensso.timbre :refer [debug info warn]]
        [clojure.core.reducers :as r]
        [clojure.core.async :as async :refer [go go-loop <! >!]])]
      :cljs
      [(:require-macros
        [cljs.core.async.macros :refer [go go-loop]]
        [cljs-log.core :refer [debug info warn]])
       (:require
        [thi.ng.xerror.core :as err]
        [clojure.core.reducers :as r]
        [cljs.core.async :as async :refer [<! >!]])]))

#?(:cljs
   (when-not (satisfies? IReduce LazyTransformer)
     (defn- seq-reduce*
       ([f coll]
        (if-let [s (seq coll)]
          (reduce f (first s) (next s))
          (f)))
       ([f val coll]
        (loop [val val, coll (seq coll)]
          (if coll
            (let [nval (f val (first coll))]
              (if (reduced? nval)
                @nval
                (recur nval (next coll))))
            val))))

     (extend-type LazyTransformer
       IReduce
       (-reduce
         ([coll f] (seq-reduce* f coll))
         ([coll f start] (seq-reduce* f start coll))))))

;;#?(:clj (taoensso.timbre/set-level! :warn))

;; (warn :free-ram (.freeMemory (Runtime/getRuntime)))

(defprotocol IVertex
  (id [_])
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-vertex! [_ v])
  (disconnect-all! [_])
  (neighbors [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_ handler])
  (score-signal [_])
  (receive-signal [_ src sig])
  (uncollected-signals [_])
  (signal-map [_])
  (set-value! [_ val])
  (update-value! [_ f]))

(defprotocol IComputeGraph
  (add-vertex! [_ val vspec])
  (remove-vertex! [_ v])
  (vertex-for-id [_ id])
  (vertices [_])
  (add-edge! [_ src dest f opts]))

(defprotocol IWatch
  (add-watch! [_ type id f])
  (remove-watch! [_ type id]))

(defprotocol IGraphComponent
  (add-to-graph! [_ g])
  (remove-from-graph! [_ g]))

(defprotocol IGraphExecutor
  (execute! [_])
  (notify! [_])
  (stop! [_]))

(deftype Vertex
    [id value state prev-val uncollected signal-map outs]
  #?@(:clj
       [clojure.lang.IDeref
        (deref
         [_] @value)]
       :cljs
       [IDeref
        (-deref
         [_] @value)])
  IVertex
  (id
    [_] id)
  (set-value!
    [_ val] (reset! value val) _)
  (update-value!
    [_ f] (swap! value f) _)
  (collect!
    [_]
    ((::collect-fn @state) _)
    (reset! uncollected [])
    _)
  (score-collect
    [_] ((::score-collect-fn @state) _))
  (connect-to!
    [_ v sig-fn opts]
    (swap! outs assoc v [sig-fn opts])
    (swap! state update ::new-edges inc)
    (debug id "edge to" (id ^Vertex v) "(" (pr-str opts) ") new:" (::new-edges @state))
    _)
  (neighbors
    [_] (keys @outs))
  (disconnect-vertex!
    [_ v]
    (debug id "disconnect from" (id ^Vertex v))
    (swap! outs dissoc v)
    _)
  (disconnect-all!
    [_]
    (run! #(disconnect-vertex! _ %) (keys @outs))
    _)
  (score-signal
    [_] ((::score-signal-fn @state) _))
  (signal!
    [_ handler]
    (reset! prev-val @value)
    (swap! state assoc ::new-edges 0)
    (handler _))
  (receive-signal
    [_ src sig]
    (if-not (= sig (@signal-map src))
      (do (swap! uncollected conj sig)
          (swap! signal-map assoc src sig)
          true)
      (debug id " ignoring unchanged signal: " (pr-str sig))))
  (signal-map
    [_] @signal-map)
  (uncollected-signals
    [_] @uncollected))

#?(:clj
   (defmethod clojure.pprint/simple-dispatch Vertex
     [^Vertex o] ((get-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap) o)))
#?(:clj
   (defmethod print-method Vertex
     [^Vertex o ^java.io.Writer w] (.write w (.toString (into {} o)))))

(defn collect-pure
  [collect-fn]
  (fn [^Vertex vertex]
    (swap! (.-value vertex) #(collect-fn % (uncollected-signals vertex)))))

(def collect-into (collect-pure into))

(defn signal-forward
  [^Vertex vertex _] @vertex)

(defn default-score-signal
  "Computes vertex signal score. Returns 0 if value equals prev-val,
  else returns 1."
  [^Vertex vertex] (if (= @vertex @(.-prev-val vertex)) 0 1))

(defn score-signal-with-new-edges
  "Computes vertex signal score. Returns number of *new* outgoing
  edges plus 1 if value not equals prev-val. New edge counter is reset
  each time signal! is called."
  [^Vertex vertex] (+ (::new-edges @(.-state vertex)) (if (= @vertex @(.-prev-val vertex)) 0 1)))

(defn default-score-collect
  "Computes vertex collect score, here simply the number of
  uncollected signal values."
  [^Vertex vertex] (count (uncollected-signals vertex)))

(defn sync-vertex-signal
  [^Vertex vertex]
  (let [id   (id vertex)
        outs @(.-outs vertex)]
    (loop [active (transient []), outs outs]
      (let [[v [f opts]] (first outs)]
        (if v
          (let [signal (f vertex opts)]
            (if-not (nil? signal)
              (if (receive-signal v id signal)
                (recur (conj! active v) (next outs))
                (recur active (next outs)))
              (do (debug "signal fn for" (id ^Vertex v) "returned nil, skipping...")
                  (recur active (next outs)))))
          (persistent! active))))))

(def default-vertex-state
  {::score-collect-fn default-score-collect
   ::score-signal-fn  default-score-signal
   ::collect-fn       collect-into
   ::new-edges        0})

(defn vertex
  [id val opts]
  (Vertex.
   id
   (atom val)
   (atom (merge default-vertex-state opts))
   (atom nil)
   (atom [])
   (atom {})
   (atom {})))

(defn notify-watches
  [watches evt]
  (loop [watches (vals (watches (first evt)))]
    (when watches
      ((first watches) evt)
      (recur (next watches)))))

(defrecord InMemoryGraph [state watches]
  IComputeGraph
  (add-vertex!
    [_ val vspec]
    (let [v (-> state
                (swap!
                 (fn [state]
                   (let [id (:next-id state)
                         v  (vertex id val vspec)]
                     (-> state
                         (update :next-id inc)
                         (update :vertices assoc id v)
                         (assoc  :curr-vertex v)))))
                :curr-vertex)]
      (notify-watches @watches [:add-vertex v])
      v))
  (remove-vertex!
    [_ v]
    (when (get-in @state [:vertices (id ^Vertex v)])
      (notify-watches @watches [:remove-vertex v])
      (swap! state update :vertices dissoc (id ^Vertex v))
      ;;(disconnect-all! v)
      true))
  (vertex-for-id
    [_ id] (get-in @state [:vertices id]))
  (vertices
    [_] (-> @state :vertices vals))
  (add-edge!
    [_ src dest sig-fn opts]
    (connect-to! src dest sig-fn opts)
    (notify-watches @watches [:add-edge src dest sig-fn opts])
    _)
  IWatch
  (add-watch!
    [_ type id f]
    (info "adding watch" type id f)
    (swap! watches assoc-in [type id] f)
    _)
  (remove-watch!
    [_ type id]
    (info "removing watch" type id)
    (swap! watches update type dissoc id)
    _))

(defn compute-graph
  [] (InMemoryGraph. (atom {:vertices {} :next-id 0}) (atom {})))

(defn- now [] #?(:clj (System/nanoTime) :cljs (.getTime (js/Date.))))

(defn execution-result
  [type sigs colls t0 & [opts]]
  (let [rt #?(:clj (* (- (now) t0) 1e-6) :cljs (- (now) t0))]
    (->> {:collections colls
          :signals     sigs
          :type        type
          :runtime     rt
          :time-per-op (if (or (pos? sigs) (pos? colls)) (/ rt (+ sigs colls)) 0)}
         (merge opts))))

(defn async-execution-result
  [type out sigs colls t0 & [opts]]
  (go (>! out (execution-result type colls sigs t0 opts))))

(defn sub-pass-combine
  ([] [#{} 0])
  ([acc] acc)
  ([[active ax] [active' x]] [(into active active') (+ ax x)]))

(defn single-pass-combine
  ([] [#{} 0 0])
  ([acc] acc)
  ([[act as ac] [act' s c]] [(into act act') (+ as s) (+ ac c)]))

(defn sync-signal-pass-simple
  [thresh]
  (fn [vertices]
    (loop [sigs 0, verts vertices]
      (if verts
        (let [v (first verts)]
          (if (> (score-signal v) thresh)
            (do (debug (id ^Vertex v) "signaling")
                (recur (long (+ sigs (count (signal! v sync-vertex-signal)))) (next verts)))
            (recur sigs (next verts))))
        sigs))))

(defn sync-collect-pass-simple
  [thresh]
  (fn [vertices]
    (loop [colls 0, verts vertices]
      (if verts
        (let [v (first verts)]
          (if (> (score-collect v) thresh)
            (do (debug (id ^Vertex v) "collecting")
                (collect! v)
                (recur (inc colls) (next verts)))
            (recur colls (next verts))))
        colls))))

(defn parallel-signal-pass-simple
  [thresh]
  (fn [vertices]
    (r/fold
     + (fn [sigs v]
         (if (> (score-signal v) thresh)
           (+ sigs (count (signal! v sync-vertex-signal)))
           sigs))
     vertices)))

(defn parallel-collect-pass-simple
  [thresh]
  (fn [vertices]
    (r/fold
     + (fn [colls v]
         (if (> (score-collect v) thresh)
           (do (collect! v) (inc colls))
           colls))
     vertices)))

(defn parallel-signal-pass
  [thresh]
  (fn [workgroup-size vertices]
    (r/fold
     workgroup-size
     sub-pass-combine
     (fn [acc v]
       (if (> (score-signal v) thresh)
         (let [vsigs (signal! v sync-vertex-signal)]
           [(into (acc 0) vsigs) (+ (acc 1) (count vsigs))])
         acc))
     vertices)))

(defn parallel-collect-pass
  [thresh]
  (fn [workgroup-size vertices]
    (r/fold
     workgroup-size
     sub-pass-combine
     (fn [acc v]
       (if (> (score-collect v) thresh)
         (do (collect! v)
             [(conj (acc 0) v) (inc (acc 1))])
         acc))
     vertices)))

(defn parallel-two-pass-processor
  [s-thresh c-thresh]
  (let [sig-fn  (parallel-signal-pass s-thresh)
        coll-fn (parallel-collect-pass c-thresh)]
    (fn [workgroup-size vertices]
      (let [#?@(:cljs [vertices (seq vertices)])
            [s-act sigs]  (sig-fn workgroup-size vertices)
            [c-act colls] (coll-fn workgroup-size vertices)]
        [(into s-act c-act) sigs colls]))))

(defn probabilistic-single-pass-processor
  [s-thresh c-thresh]
  (fn [[active sigs colls] v]
    (let [active (conj active v)]
      (if (< (rand) 0.5)
        (if (> (score-signal v) s-thresh)
          (let [vsigs (signal! v sync-vertex-signal)]
            [(into active vsigs) (+ sigs (count vsigs)) colls])
          [active sigs colls])
        (if (> (score-collect v) c-thresh)
          (do (collect! v)
              [active sigs (inc colls)])
          [active sigs colls])))))

(defn eager-probabilistic-single-pass-processor
  [s-thresh c-thresh]
  (fn [[active sigs colls] v]
    (let [active (conj active v)]
      (if (< (rand) 0.5)
        (if (> (score-signal v) s-thresh)
          (let [vsigs (signal! v sync-vertex-signal)]
            [(into active vsigs) (+ sigs (count vsigs)) colls])
          [active sigs colls])
        (if (> (score-collect v) c-thresh)
          (do (collect! v)
              (if (> (score-signal v) s-thresh)
                (let [vsigs (signal! v sync-vertex-signal)]
                  [(into active vsigs) (+ sigs (count vsigs)) (inc colls)])
                [active sigs (inc colls)]))
          [active sigs colls])))))

(defn single-pass-scheduler
  [ctx]
  (let [processor ((:processor ctx) (:signal-thresh ctx) (:collect-thresh ctx))]
    (fn [workgroup-size vertices]
      (r/fold workgroup-size single-pass-combine processor #?(:clj vertices :cljs (seq vertices))))))

(defn two-pass-scheduler
  [ctx]
  ((:processor ctx) (:signal-thresh ctx) (:collect-thresh ctx)))

(defn add-context-watches
  [g watch-id queue]
  (add-watch! g :add-vertex watch-id
              (fn [[__ v]] (swap! queue into (signal! v sync-vertex-signal))))
  (add-watch! g :remove-vertex watch-id
              (fn [[__ v]] (swap! queue into (signal! v sync-vertex-signal))))
  (add-watch! g :add-edge watch-id
              (fn [[__ src dest]] (swap! queue into [src dest]))))

(defn remove-context-watches
  [g watch-id]
  (remove-watch! g :add-vertex watch-id)
  (remove-watch! g :remove-vertex watch-id)
  (remove-watch! g :add-edge watch-id))

(defn default-sync-context-opts
  []
  {:collect-thresh 0
   :signal-thresh  0
   :max-iter       1e6
   :signal-fn      parallel-signal-pass-simple
   :collect-fn     parallel-collect-pass-simple})

(defn sync-execution-context
  [opts]
  (let [ctx       (merge (default-sync-context-opts) opts)
        g         (:graph ctx)
        coll-fn   ((:collect-fn ctx) (:collect-thresh ctx))
        sig-fn    ((:signal-fn ctx) (:signal-thresh ctx))
        max-iter  (:max-iter ctx)
        watch-id  (keyword (gensym))
        ->result  (fn [type iter sigs colls t0]
                    (remove-watch! g :remove-vertex watch-id)
                    (execution-result type sigs colls t0 {:iterations iter}))]
    (reify
      #?@(:clj
           [clojure.lang.IDeref
            (deref [_] ctx)]
           :cljs
           [IDeref
            (-deref [_] ctx)])
      IGraphExecutor
      (stop! [_] (err/unsupported!))
      (notify! [_] (err/unsupported!))
      (execute!
        [_]
        (add-watch! g :remove-vertex watch-id
                    (fn [[__ v]] (signal! v sync-vertex-signal)))
        (let [t0 (now)]
          (loop [i 0, colls 0, sigs 0]
            (if (<= i max-iter)
              (let [verts  (vertices g)
                    sigs'  (sig-fn verts)
                    colls' (coll-fn verts)]
                (if (or (pos? sigs') (pos? colls'))
                  (recur (inc i) (long (+ colls colls')) (long (+ sigs sigs')))
                  (->result :converged i sigs colls t0)))
              (->result :max-iter-reached i sigs colls t0))))))))

(defn default-scheduled-context-opts
  []
  {:collect-thresh 0
   :signal-thresh  0
   :processor      parallel-two-pass-processor
   :scheduler      two-pass-scheduler
   ;;:processor      eager-probabilistic-single-pass-processor
   ;;:scheduler      single-pass-scheduler
   :max-ops        1e6
   :threads        #?(:clj (inc (.availableProcessors (Runtime/getRuntime))) :cljs 1)})

(defn scheduled-execution-context
  [opts]
  (let [ctx       (merge (default-scheduled-context-opts) opts)
        g         (:graph ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        scheduler ((:scheduler ctx) ctx)
        threads   (:threads ctx)
        watch-id  (keyword (gensym))
        v-filter  (filter #(or (> (score-signal %) s-thresh) (> (score-collect %) c-thresh)))
        ->result  (fn [type sigs colls t0]
                    (remove-context-watches g watch-id)
                    (execution-result type sigs colls t0))]
    (reify
      #?@(:clj
           [clojure.lang.IDeref
            (deref [_] ctx)]
           :cljs
           [IDeref
            (-deref [_] ctx)])
      IGraphExecutor
      (stop! [_] (err/unsupported!))
      (notify! [_] (err/unsupported!))
      (execute!
        [_]
        (let [t0     (now)
              active (atom (sequence v-filter (vertices g)))
              queue  (atom #{})]
          (add-context-watches g watch-id queue)
          (loop [colls 0, sigs 0]
            ;;(warn :active-count (count @active))
            (if (seq @active)
              (if (<= (+ colls sigs) max-ops)
                (let [grp-size            (max 512 (long (/ (count @active) threads)))
                      [act' sigs' colls'] (scheduler grp-size @active)]
                  ;;(warn :auto (count @queue))
                  (reset! active (into (into #{} v-filter act') @queue))
                  (reset! queue #{})
                  (recur (long (+ colls colls')) (long (+ sigs sigs'))))
                (->result :max-ops-reached sigs colls t0))
              (->result :converged sigs colls t0))))))))

(defn default-async-context-opts
  []
  {:collect-thresh 0
   :signal-thresh  0
   :processor      parallel-two-pass-processor
   :scheduler      two-pass-scheduler
   ;;:processor      eager-probabilistic-single-pass-processor
   ;;:scheduler      single-pass-scheduler
   :max-ops        1e6
   :threads        #?(:clj (inc (.availableProcessors (Runtime/getRuntime))) :cljs 1)})

(defn async-execution-context
  [opts]
  (let [ctx         (merge (default-async-context-opts) opts)
        g           (:graph ctx)
        c-thresh    (:collect-thresh ctx)
        s-thresh    (:signal-thresh ctx)
        max-ops     (:max-ops ctx)
        scheduler   ((:scheduler ctx) ctx)
        threads     (:threads ctx)
        res-chan    (or (:result ctx) (async/chan))
        notify-chan (async/chan (async/dropping-buffer 1))
        watch-id    (keyword (gensym))
        v-filter    (filter #(or (> (score-signal %) s-thresh) (> (score-collect %) c-thresh)))
        ->result    (fn [type sigs colls t0] (go (>! res-chan (execution-result type sigs colls t0))))]
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
        (remove-context-watches g watch-id)
        (async/close! notify-chan))
      (notify!
        [_] (go (>! notify-chan true)) nil)
      (execute!
        [_]
        (let [t0     (atom (now))
              active (atom (sequence v-filter (vertices g)))
              queue  (atom #{})]
          (add-context-watches g watch-id queue)
          (go-loop [colls 0, sigs 0]
            ;;(warn :active-count (count @active))
            (if (seq @active)
              (if (<= (+ colls sigs) max-ops)
                (let [grp-size            (max 512 (long (/ (count @active) threads)))
                      [act' sigs' colls'] (scheduler grp-size @active)]
                  ;;(warn :auto (count @queue))
                  (reset! active (into (into #{} v-filter act') @queue))
                  (reset! queue #{})
                  (recur (long (+ colls colls')) (long (+ sigs sigs'))))
                (do (stop! _)
                    (->result :max-ops-reached sigs colls @t0)))
              (do (->result :converged sigs colls @t0)
                  (if (:auto-stop ctx)
                    (stop! _)
                    (when (<! notify-chan)
                      (reset! t0 (now))
                      (reset! active (sequence v-filter (vertices g)))
                      (reset! queue #{})
                      ;;(warn :rerun (count @active))
                      (recur 0 0))))))
          res-chan)))))
