(ns thi.ng.fabric.async
  (:require
   [clojure.core.async :as a :refer [go go-loop chan close! <! >! alts! timeout]]
   [taoensso.timbre :refer [debug info warn error]]))

(taoensso.timbre/set-level! :debug)

;;(warn :free-ram (.freeMemory (Runtime/getRuntime)))

(defprotocol IVertex
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-vertex! [_ v])
  (disconnect-all! [_])
  (close-input! [_])
  (input-channel [_])
  (connected-vertices [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_])
  (score-signal [_])
  (receive-signal [_ src sig])
  (set-value! [_ val])
  (update-value! [_ f]))

(defprotocol IComputeGraph
  (add-vertex! [_ vstate])
  (remove-vertex! [_ v])
  (vertex-for-id [_ id])
  (vertices [_])
  (add-edge! [_ src dest f opts]))

(defprotocol IGraphExecutor
  (execute! [_])
  (stop! [_])
  (notify! [_ evt])
  (include-vertex! [_ v]))

(defn simple-collect
  [collect-fn]
  (fn [vertex]
    (swap! (:state vertex) #(update % :val collect-fn (::uncollected %)))))

(def default-collect (simple-collect into))

(defn signal-forward
  [vertex _] @vertex)

(defn default-score-signal
  "Computes vertex signal score. Returns 0 if state's :val
  equals :last-sig-state, else returns 1."
  [{:keys [state last-sig-state]}]
  (if (= (:val @state) @last-sig-state) 0 1))

(defn default-score-collect
  "Computes vertex collect score, here simply the number
  of ::uncollected signal values."
  [vertex] (-> vertex :state deref ::uncollected count))

(defn should-signal?
  [vertex thresh] (> (score-signal vertex) thresh))

(defn should-collect?
  [vertex thresh] (> (score-collect vertex) thresh))

(def default-vertex-state
  {::uncollected      []
   ::signal-map       {}
   ::score-collect-fn default-score-collect
   ::score-signal-fn  default-score-signal
   ::collect-fn       default-collect
   ::input-channel-fn chan})

(defrecord Vertex [id state last-sig-state in outs]
  clojure.lang.IDeref
  (deref
    [_] (:val @state))
  IVertex
  (set-value!
    [_ val] (swap! state assoc :val val) (signal! _) _)
  (update-value!
    [_ f] (swap! state update :val f) (signal! _) _)
  (collect!
    [_]
    ((::collect-fn @state) _)
    (swap! state assoc ::uncollected [])
    _)
  (score-collect
    [_] ((::score-collect-fn @state) _))
  (connect-to!
    [_ v sig-fn opts]
    (debug (format "%d connecting to %d (%s)" id (:id v) (pr-str opts)))
    (swap! outs assoc v [sig-fn opts]) _)
  (connected-vertices
    [_] (keys @outs))
  (disconnect-vertex!
    [_ v]
    (debug (format "%d disconnecting from %d" id (:id v)))
    (swap! outs dissoc v)
    _)
  (disconnect-all!
    [_] (run! #(disconnect-vertex! _ %) (keys @outs)) _)
  (close-input! [_]
    (if @in
      (do #_(debug (str id " closing..."))
          (close! @in)
          (reset! in nil))
      (warn (str id " already closed")))
    _)
  (input-channel
    [_] (or @in (reset! in ((::input-channel-fn @state)))))
  (score-signal
    [_] ((::score-signal-fn @state) _))
  (signal!
    [_]
    (let [state @state]
      (reset! last-sig-state (:val state))
      (go-loop [outs @outs]
        (let [[v [f opts]] (first outs)]
          (when v
            (let [signal (f _ opts)]
              (if-not (nil? signal)
                (>! (input-channel v) [id signal])
                (warn (format "signal fn for %d returned nil, skipping..." (:id v)))))
            (recur (next outs)))))
      _))
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
  [id state]
  (map->Vertex
   {:id             id
    :state          (atom (merge default-vertex-state state))
    :last-sig-state (atom nil)
    :in             (atom nil)
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

(defrecord LoggedGraph [g logger]
  IComputeGraph
  (add-vertex! [_ v]
    (let [v (add-vertex! g v)]
      (go (>! logger [:add-vertex (:id v) @v]))
      v))
  (remove-vertex! [_ v]
    (when (remove-vertex! g v)
      (go (>! logger [:remove-vertex (:id v)]))
      true))
  (vertex-for-id
    [_ id] (vertex-for-id g id))
  (vertices
    [_] (vertices g))
  (add-edge!
    [_ src dest f opts]
    (connect-to! src dest f opts)
    (go (>! logger [:add-edge (:id src) (:id dest) f opts]))))

(defn compute-graph
  [] (Graph. (atom {:vertices {} :next-id 0})))

(defn logged-compute-graph
  [logger] (LoggedGraph. (compute-graph) logger))

(defn eager-vertex-processor
  [{:keys [id] :as vertex} ctx]
  (let [{:keys [collect-thresh signal-thresh]} @ctx
        in (input-channel vertex)]
    (go-loop []
      (let [[src-id sig] (<! in)]
        (if sig
          (do (debug (format "%d receive from %d: %s" id src-id (pr-str sig)))
              (receive-signal vertex src-id sig)
              (when (should-collect? vertex collect-thresh)
                (notify! ctx [:collect id])
                (collect! vertex)
                ;;(debug (format "%d post-collection: %s" id (pr-str @vertex)))
                (when (should-signal? vertex signal-thresh)
                  (notify! ctx [:signal id])
                  (signal! vertex)))
              (recur))
          #_(debug (str id " stopped")))))
    vertex))

(defn execution-result
  [type p colls sigs t0 & [opts]]
  (->> {:collections colls
        :signals     sigs
        :type        type
        :runtime     (* (- (System/nanoTime) t0) 1e-6)}
       (merge opts)
       (deliver p)))

(defn stop-execution
  [bus vertices]
  (run! close! bus)
  (run! close-input! vertices))

(def default-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :processor      eager-vertex-processor
   :bus-size       16
   :timeout        25
   :max-ops        1e6})

(defn async-context
  [opts]
  (let [ctx       (merge default-context-opts opts)
        bus       (vec (repeatedly (:bus-size ctx) chan))
        ctx       (assoc ctx :bus bus)
        processor (:processor ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        max-t     (:timeout ctx)
        result    (promise)]
    (reify
      clojure.lang.IDeref
      (deref [_] ctx)
      IGraphExecutor
      (stop!
        [_]
        (stop-execution bus (vertices (:graph ctx)))
        result)
      (notify! [_ evt]
        (go (>! (rand-nth bus) evt)))
      (include-vertex!
        [_ v]
        (processor v _)
        (when (should-collect? v c-thresh)
          (collect! v))
        (when (should-signal? v s-thresh)
          (signal! v))
        _)
      (execute! [_]
        (if-not (realized? result)
          (let [t0 (System/nanoTime)]
            (go
              (run! #(include-vertex! _ %) (vertices (:graph ctx)))
              (loop [colls 0 sigs 0]
                (if (<= (+ colls sigs) max-ops)
                  (let [t (if max-t (timeout max-t))
                        [[evt v ex] port] (alts! (if max-t (conj bus t) bus))]
                    (if (= port t)
                      (do (stop! _)
                          (execution-result :converged result colls sigs t0))
                      (case evt
                        :collect (recur (inc colls) sigs)
                        :signal  (recur colls (inc sigs))
                        :error   (do (warn ex "@ vertex" v)
                                     (stop! _)
                                     (execution-result
                                      :error result colls sigs t0
                                      {:reason           :error
                                       :reason-event     evt
                                       :reason-exception ex
                                       :reason-vertex    v}))
                        (do (warn "execution interrupted")
                            (execution-result
                             :interrupted result colls sigs t0
                             {:reason        :unknown
                              :reason-event  evt
                              :reason-vertex v})))))
                  (execution-result :terminated result colls sigs t0))))
            result)
          (throw (IllegalStateException. "Context already executed once")))))))

(defn dot
  [path g flt]
  (->> (vertices g)
       (filter flt)
       (mapcat
        (fn [v]
          (if-let [outs @(:outs v)]
            (->> outs
                 (map
                  (fn [[k [_ opts]]]
                    (str (:id v) "->" (:id k) "[label=\"" (pr-str opts) "\"];\n")))
                 (cons
                  (format
                   "%d[label=\"%d (%s)\"];\n"
                   (:id v) (:id v) (pr-str @v)))))))
       (apply str)
       (format "digraph g {
node[color=black,style=filled,fontname=Inconsolata,fontcolor=white,fontsize=9];
edge[fontname=Inconsolata,fontsize=9];
ranksep=1;
overlap=scale;
%s}")
       (spit path)))

(defn signal-sssp
  [vertex dist] (if-let [v @vertex] (+ dist v)))

(def collect-sssp
  (simple-collect
   (fn [val uncollected]
     (if val (reduce min val uncollected) (reduce min uncollected)))))

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

(defn sssp-test-linked
  [g num-verts num-edges e-length]
  (let [vspec {::collect-fn collect-sssp}
        verts (->> (range num-verts)
                   (map (fn [_] (add-vertex! g vspec)))
                   (cons (add-vertex! g (assoc vspec :val 0)))
                   vec)]
    (dotimes [i num-edges]
      (->> (make-strand verts e-length)
           (partition 2 1)
           (map (fn [[a b]] (add-edge! g (verts a) (verts b) signal-sssp 1)))
           (doall)))
    nil))

(comment
  (def g (compute-graph))
  (def ctx
    (async-context
     {:graph     g
      :bus-size  64
      :processor eager-vertex-processor}))
  (def a (add-vertex! g {:val 0   ::collect-fn collect-sssp}))
  (def b (add-vertex! g {:val nil ::collect-fn collect-sssp}))
  (def c (add-vertex! g {:val nil ::collect-fn collect-sssp}))
  (def d (add-vertex! g {:val nil ::collect-fn collect-sssp}))

  (add-edge! g a b signal-sssp 1)
  (add-edge! g a c signal-sssp 3)
  (add-edge! g b c signal-sssp 1)
  (add-edge! g c d signal-sssp 1)

  (prn @(execute! ctx))

  (prn @a)
  (prn @b)
  (prn @c)
  (prn @d))

(comment
  (def g (compute-graph))
  (def ctx
    (async-context
     {:graph     g
      :processor eager-vertex-processor}))

  (def types
    '[animal vertebrae mammal human dog fish shark plant tree oak fir])

  (def hierarchy
    '[[animal vertebrae]
      [vertebrae mammal]
      [vertebrae fish]
      [fish shark]
      [mammal human]
      [mammal dog]
      [plant tree]
      [tree oak]
      [tree fir]])

  (def rdfs-collect
    (simple-collect
     (fn [val incoming]
       (reduce into val incoming))))

  (def verts
    (let [vspec {::collect-fn rdfs-collect
                 ;;::score-collect-fn (constantly 1)
                 ;;::score-signal-fn (constantly 1)
                 }
          verts (reduce
                 (fn [acc v] (assoc acc v (add-vertex! g (assoc vspec :val #{v}))))
                 {} types)]
      (doseq [[a b] hierarchy]
        (add-edge! g (verts a) (verts b) signal-forward nil))
      verts)))

;;(warn :free-ram (.freeMemory (Runtime/getRuntime)))
