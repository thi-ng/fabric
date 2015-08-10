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

;;#?(:clj (taoensso.timbre/set-level! :warn))

;; (warn :free-ram (.freeMemory (Runtime/getRuntime)))

(defprotocol IVertex
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-vertex! [_ v])
  (disconnect-all! [_])
  (connected-vertices [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_ handler])
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

(defprotocol IWatchable
  (add-watch! [_ type id f])
  (remove-watch! [_ type id]))

(defprotocol IGraphExecutor
  (execute! [_])
  (stop! [_])
  (notify! [_])
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

(defn score-signal-with-new-edges
  "Computes vertex signal score. Returns number of *new* outgoing
  edges plus 1 if value not equals prev-val. New edge counter is reset
  each time signal! is called."
  [vertex]
  (+ (::new-edges @(:state vertex)) (if (= @vertex @(:prev-val vertex)) 0 1)))

(defn default-score-collect
  "Computes vertex collect score, here simply the number
  of ::uncollected signal values."
  [vertex] (-> vertex :state deref ::uncollected count))

(defn should-signal?
  [vertex thresh] (> (score-signal vertex) thresh))

(defn should-collect?
  [vertex thresh] (> (score-collect vertex) thresh))

(defn sync-vertex-signal
  [vertex]
  (let [id   (:id vertex)
        outs @(:outs vertex)]
    (loop [active (transient []), outs outs]
      (let [[v [f opts]] (first outs)]
        (if v
          (let [signal (f vertex opts)]
            (if-not (nil? signal)
              (if (receive-signal v id signal)
                (recur (conj! active v) (next outs))
                (recur active (next outs)))
              (do (debug "signal fn for" (:id v) "returned nil, skipping...")
                  (recur active (next outs)))))
          (persistent! active))))))

(def default-vertex-state
  {::uncollected      []
   ::signal-map       {}
   ::score-collect-fn default-score-collect
   ::score-signal-fn  default-score-signal
   ::collect-fn       collect-into
   ::new-edges        0})

(defrecord Vertex [id value state prev-val outs]
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
    [_ val] (reset! value val) _)
  (update-value!
    [_ f] (swap! value f) _)
  (collect!
    [_]
    ((::collect-fn @state) _)
    (swap! state assoc ::uncollected [])
    _)
  (score-collect
    [_] ((::score-collect-fn @state) _))
  (connect-to!
    [_ v sig-fn opts]
    (swap! outs assoc v [sig-fn opts])
    (swap! state update ::new-edges inc)
    (debug id "edge to" (:id v) "(" (pr-str opts) ") new:" (::new-edges @state))
    _)
  (connected-vertices
    [_] (keys @outs))
  (disconnect-vertex!
    [_ v]
    (debug id "disconnect from" (:id v))
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
    (if-not (= sig ((::signal-map @state) src))
      (do (swap! state
                 #(-> %
                      (update ::uncollected conj sig)
                      (assoc-in [::signal-map src] sig)))
          true)
      (debug id " ignoring unchanged signal: " (pr-str sig)))))

#?(:clj
   (defmethod clojure.pprint/simple-dispatch Vertex
     [^Vertex o] ((get-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap) o)))
#?(:clj
   (defmethod print-method Vertex
     [^Vertex o ^java.io.Writer w] (.write w (.toString (into {} o)))))

(defn vertex
  [id val opts]
  (map->Vertex
   {:id       id
    :value    (atom val)
    :state    (atom (merge default-vertex-state opts))
    :prev-val (atom nil)
    :outs     (atom {})}))

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
    (when (get-in @state [:vertices (:id v)])
      (notify-watches @watches [:remove-vertex v])
      (swap! state update :vertices dissoc (:id v))
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
  IWatchable
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
  (->> {:collections colls
        :signals     sigs
        :type        type
        :runtime     #?(:clj (* (- (now) t0) 1e-6) :cljs (- (now) t0))}
       (merge opts)))

(defn async-execution-result
  [type out colls sigs t0 & [opts]]
  (go (>! out (execution-result type colls sigs t0 opts))))

(defn sync-signal-vertices
  [vertices thresh]
  (loop [sigs 0, verts vertices]
    (if verts
      (let [v (first verts)]
        (if (should-signal? v thresh)
          (do (debug (:id v) "signaling")
              (recur (long (+ sigs (count (signal! v sync-vertex-signal)))) (next verts)))
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
   + (fn [sigs v]
       (if (should-signal? v thresh)
         (+ sigs (count (signal! v sync-vertex-signal)))
         sigs))
   vertices))

(defn parallel-sync-collect-vertices
  [vertices thresh]
  (r/fold
   + (fn [colls v]
       (if (should-collect? v thresh)
         (do (collect! v) (inc colls))
         colls))
   vertices))

(defn default-sync-context-opts
  []
  {:collect-thresh 0
   :signal-thresh  0
   :max-iter       1e6
   :signal-fn      parallel-sync-signal-vertices
   :collect-fn     parallel-sync-collect-vertices})

(defn sync-execution-context
  [opts]
  (let [ctx       (merge (default-sync-context-opts) opts)
        g         (:graph ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        coll-fn   (:collect-fn ctx)
        sig-fn    (:signal-fn ctx)
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
      (include-vertex! [_ v] (err/unsupported!))
      (execute!
        [_]
        (add-watch! g :remove-vertex watch-id
                    (fn [[__ v]] (signal! v sync-vertex-signal)))
        (let [t0 (now)]
          (loop [i 0, colls 0, sigs 0]
            (if (<= i max-iter)
              (let [verts  (vertices g)
                    sigs'  (sig-fn verts s-thresh)
                    colls' (coll-fn verts c-thresh)]
                (if (or (pos? sigs') (pos? colls'))
                  (recur (inc i) (long (+ colls colls')) (long (+ sigs sigs')))
                  (->result :converged i sigs colls t0)))
              (->result :max-iter-reached i sigs colls t0))))))))

(defn probabilistic-vertex-processor
  [s-thresh c-thresh]
  (fn [[active sigs colls] v]
    (let [active (conj active v)]
      (if (< (rand) 0.5)
        (if (should-signal? v s-thresh)
          (let [vsigs (signal! v sync-vertex-signal)]
            [(into active vsigs) (+ sigs vsigs) colls])
          [active sigs colls])
        (if (should-collect? v c-thresh)
          (do (collect! v)
              [active sigs (inc colls)])
          [active sigs colls])))))

(defn eager-probabilistic-vertex-processor
  [s-thresh c-thresh]
  (fn [[active sigs colls] v]
    (let [active (conj active v)]
      (if (< (rand) 0.5)
        (if (should-signal? v s-thresh)
          (let [vsigs (signal! v sync-vertex-signal)]
            [(into active vsigs) (+ sigs (count vsigs)) colls])
          [active sigs colls])
        (if (should-collect? v c-thresh)
          (do (collect! v)
              (if (should-signal? v s-thresh)
                (let [vsigs (signal! v sync-vertex-signal)]
                  [(into active vsigs) (+ sigs (count vsigs)) (inc colls)])
                [active sigs (inc colls)]))
          [active sigs colls])))))

(defn combine-stats
  ([] [#{} 0 0])
  ([acc] acc)
  ([[act as ac] [act' s c]] [(into act act') (+ as s) (+ ac c)]))

(defn single-pass-scheduler
  [workgroup-size processor vertices]
  (r/fold workgroup-size combine-stats processor vertices))

(defn two-pass-scheduler
  [workgroup-size processor vertices]
  )

(defn default-async-context-opts
  []
  {:collect-thresh 0
   :signal-thresh  0
   :processor      eager-probabilistic-vertex-processor
   :scheduler      single-pass-scheduler
   :max-ops        1e6
   :threads        #?(:clj (inc (.availableProcessors (Runtime/getRuntime))) :cljs 1)})

(defn execution-context
  [opts]
  (let [ctx       (merge (default-async-context-opts) opts)
        g         (:graph ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        processor ((:processor ctx) s-thresh c-thresh)
        scheduler (:scheduler ctx)
        threads   (:threads ctx)
        watch-id  (keyword (gensym))
        v-filter  (filter #(or (should-signal? % s-thresh) (should-collect? % c-thresh)))
        ->result  (fn [type sigs colls t0]
                    (remove-watch! g :add-vertex watch-id)
                    (remove-watch! g :remove-vertex watch-id)
                    (remove-watch! g :add-edge watch-id)
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
      (include-vertex! [_ v] (err/unsupported!))
      (execute!
        [_]
        (let [t0 (now)
              active (atom (sequence v-filter (vertices g)))
              enable (atom #{})]
          (add-watch! g :add-vertex watch-id
                      (fn [[__ v]] (swap! enable into (signal! v sync-vertex-signal))))
          (add-watch! g :remove-vertex watch-id
                      (fn [[__ v]] (swap! enable into (signal! v sync-vertex-signal))))
          (add-watch! g :add-edge watch-id
                      (fn [[__ src dest]] (swap! enable into [src dest])))
          (loop [colls 0, sigs 0]
            ;;(warn :active-count (count @active))
            (if (seq @active)
              (if (<= (+ colls sigs) max-ops)
                (let [grp-size            (max 512 (long (/ (count @active) threads)))
                      [act' sigs' colls'] (scheduler grp-size processor @active)]
                  ;;(warn :auto (count @enable))
                  (reset! active (into (into #{} v-filter act') @enable))
                  (reset! enable #{})
                  (recur (long (+ colls colls')) (long (+ sigs sigs'))))
                (->result :max-ops-reached sigs colls t0))
              (->result :converged sigs colls t0))))))))

(defn async-execution-context
  [opts]
  (let [ctx         (merge (default-async-context-opts) opts)
        g           (:graph ctx)
        c-thresh    (:collect-thresh ctx)
        s-thresh    (:signal-thresh ctx)
        max-ops     (:max-ops ctx)
        processor   ((:processor ctx) s-thresh c-thresh)
        threads     (:threads ctx)
        res-chan    (or (:result ctx) (async/chan))
        notify-chan (async/chan (async/dropping-buffer 1))
        watch-id    (keyword (gensym))
        v-filter    (filter #(or (should-signal? % s-thresh) (should-collect? % c-thresh)))
        ->result    (fn [type sigs colls t0]
                      (warn :send-result type)
                      (go (>! res-chan (execution-result type sigs colls t0))))]
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
        (remove-watch! g :add-vertex watch-id)
        (remove-watch! g :remove-vertex watch-id)
        (remove-watch! g :add-edge watch-id)
        (async/close! notify-chan))
      (notify!
        [_] (go (>! notify-chan true)) nil)
      (execute!
        [_]
        (let [t0     (atom (now))
              active (atom (sequence v-filter (vertices g)))
              enable (atom #{})]
          (add-watch! g :add-vertex watch-id
                      (fn [[__ v]]
                        (swap! enable into (signal! v sync-vertex-signal))))
          (add-watch! g :remove-vertex watch-id
                      (fn [[__ v]]
                        (swap! enable into (signal! v sync-vertex-signal))))
          (add-watch! g :add-edge watch-id
                      (fn [[__ src dest]]
                        (swap! enable into [src dest])))
          (go-loop [colls 0, sigs 0]
            ;;(warn :active-count (count @active))
            (if (seq @active)
              (if (<= (+ colls sigs) max-ops)
                (let [[act' sigs' colls']
                      (r/fold
                       (max 512 (long (/ (count @active) threads)))
                       combine-stats
                       processor @active)]
                  ;;(warn :auto (count @enable))
                  (reset! active (into (into #{} v-filter act') @enable))
                  (reset! enable #{})
                  (recur (long (+ colls colls')) (long (+ sigs sigs'))))
                (do (stop! _)
                    (->result :max-ops-reached sigs colls @t0)))
              (do (->result :converged sigs colls @t0)
                  (if (:auto-stop ctx)
                    (stop! _)
                    (when (<! notify-chan)
                      (reset! t0 (now))
                      (reset! active (sequence v-filter (vertices g)))
                      (reset! enable #{})
                      ;;(warn :rerun (count @active))
                      (recur 0 0))))))
          res-chan)))))
