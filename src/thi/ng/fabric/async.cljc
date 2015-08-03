(ns thi.ng.fabric.async
  (:require
   [clojure.core.async :as a :refer [go go-loop chan close! <! >! alts! timeout]]
   [taoensso.timbre :refer [debug info warn error]]
   [clojure.set :as set]))

(taoensso.timbre/set-level! :warn)

;;(warn :free-ram (.freeMemory (Runtime/getRuntime)))

(defprotocol IVertex
  (connect-to! [_ v sig-fn edge-opts])
  (disconnect-vertex! [_ v])
  (close-input! [_])
  (input-channel [_])
  (collect! [_])
  (score-collect [_])
  (signal! [_])
  (score-signal [_])
  (receive-signal [_ src sig]))

(defprotocol IComputeGraph
  (add-vertex! [_ vstate])
  (remove-vertex! [_ v])
  (vertex-for-id [_ id])
  (vertices [_]))

(defprotocol IGraphExecutor
  (execute! [_])
  (stop! [_])
  (notify [_ evt]))

(defn simple-collect
  [collect-fn]
  (fn [vertex]
    (swap! (:state vertex) #(update % ::val collect-fn (::uncollected %)))))

(def default-collect (simple-collect into))

(defn signal-forward
  [vertex _] @vertex)

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
  (deref
    [_] (::val @state))
  IVertex
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
  (disconnect-vertex!
    [_ v]
    (debug (format "%d disconnecting from %d" id (:id v)))
    (swap! outs dissoc v)
    _)
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
      (reset! last-sig-state (::val state))
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
    (swap! state
           #(-> %
                (update ::uncollected conj sig)
                (assoc-in [::signal-map src] sig)))
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
   ::collect-fn       default-collect
   ::input-channel-fn chan})

(def default-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :bus-size       16
   :timeout        25
   :max-ops        1e6})

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
    [_ v] (swap! state update :vertices dissoc (:id v)))
  (vertex-for-id
    [_ id] (get-in @state [:vertices id]))
  (vertices
    [_] (-> @state :vertices vals)))

(defn compute-graph
  [] (Graph. (atom {:vertices {} :next-id 0})))

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
                (notify ctx [:collect id])
                (collect! vertex)
                ;;(debug (format "%d post-collection: %s" id (pr-str @vertex)))
                (when (should-signal? vertex signal-thresh)
                  (notify ctx [:signal id])
                  (signal! vertex)))
              (recur))
          #_(debug (str id " stopped")))))
    vertex))

(defn execution-result
  [type p colls sigs t0 & [opts]]
  (->> {:collections colls
        :signals     sigs
        :type        type
        :time        (* (- (System/nanoTime) t0) 1e-6)}
       (merge opts)
       (deliver p)))

(defn stop-execution
  [bus vertices]
  (run! close! bus)
  (run! close-input! vertices))

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
      (notify [_ evt]
        (go (>! (rand-nth bus) evt)))
      (execute! [_]
        (if-not (realized? result)
          (let [t0 (System/nanoTime)]
            (go
              (loop [verts (vertices (:graph ctx))]
                (when-let [v (first verts)]
                  (processor v _)
                  (when (should-collect? v c-thresh)
                    (collect! v))
                  (when (should-signal? v s-thresh)
                    (signal! v))
                  (recur (next verts))))
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
                    (str (:id v) "->" (:id k) "[label=" (pr-str opts) "];\n")))
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

(def g (compute-graph))
(def ctx
  (async-context
   {:graph     g
    :bus-size  64
    :processor eager-vertex-processor}))

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
                   (cons (add-vertex! g (assoc vspec ::val 0)))
                   vec)]
    (dotimes [i num-edges]
      (->> (make-strand verts e-length)
           (partition 2 1)
           (map (fn [[a b]] (connect-to! (verts a) (verts b) signal-sssp 1)))
           (doall)))
    nil))

(comment
  (def a (add-vertex! g {::val 0   ::collect-fn collect-sssp}))
  (def b (add-vertex! g {::val nil ::collect-fn collect-sssp}))
  (def c (add-vertex! g {::val nil ::collect-fn collect-sssp}))
  (def d (add-vertex! g {::val nil ::collect-fn collect-sssp}))

  (connect-to! a b signal-sssp 1)
  (connect-to! a c signal-sssp 3)
  (connect-to! b c signal-sssp 1)
  (connect-to! c d signal-sssp 1)

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
                 (fn [acc v] (assoc acc v (add-vertex! g (assoc vspec ::val #{v}))))
                 {} types)]
      (doseq [[a b] hierarchy]
        (connect-to! (verts a) (verts b) signal-forward nil))
      verts)))

(defn signal-triple
  [vertex _]
  [(:id vertex) @vertex])

(defn collect-index
  [spo]
  (simple-collect
   (fn [val incoming]
     (transduce
      (map (fn [[id t]] [id (nth t spo)]))
      (completing (fn [acc [id x]] (update acc x (fnil conj #{}) id)))
      val incoming))))

(defn index-vertex
  [g spo]
  (add-vertex! g {::val {}
                  ::collect-fn (collect-index spo)
                  ::score-signal-fn (constantly 1)}))

(def s-idx (index-vertex g 0))
(def p-idx (index-vertex g 1))
(def o-idx (index-vertex g 2))

(defn add-triple
  [g t]
  (let [v (add-vertex! g {::val t})]
    (connect-to! v s-idx signal-triple nil)
    (connect-to! v p-idx signal-triple nil)
    (connect-to! v o-idx signal-triple nil)
    (signal! v)
    v))

(defn signal-select
  [vertex [idx sel]]
  [idx (if sel (@vertex sel [nil]) (->> @vertex vals (eduction (mapcat identity))))])

(def collect-select
  (simple-collect
   (fn [val incoming]
     (reduce (fn [acc [idx res]] (update acc idx (fnil into #{}) res)) val incoming))))

(def aggregate-select
  (simple-collect
   (fn [val incoming]
     (let [res (vals (last incoming))]
       ;;(info :res res)
       (when (and (seq res) (every? #(not= #{nil} %) res))
         (->> res
              (map #(disj % nil))
              (set)
              (sort-by count)
              ;;(#(do (info :sorted %) %))
              (reduce set/intersection)
              (map #(deref (vertex-for-id g %)))))))))

;; TODO figure out way how to at to a running context
(defn register-query
  [g s p o]
  (let [acc (add-vertex! g {::val {} ::collect-fn collect-select})
        res (add-vertex! g {::val nil ::collect-fn aggregate-select})]
    (connect-to! s-idx acc signal-select [0 s])
    (connect-to! p-idx acc signal-select [1 p])
    (connect-to! o-idx acc signal-select [2 o])
    (connect-to! acc res signal-forward nil)
    ;;[acc res]
    res))

#_(defn select
  [g s p o]
  (let [acc (add-vertex! g {::val {} ::collect-fn collect-select})
        ctx (async-context
             {:graph     g
              :processor eager-vertex-processor})]
    (connect-to! s-idx acc signal-select [0 s])
    (connect-to! p-idx acc signal-select [1 p])
    (connect-to! o-idx acc signal-select [2 o])
    (info @(execute! ctx))
    (disconnect-vertex! s-idx acc)
    (disconnect-vertex! p-idx acc)
    (disconnect-vertex! o-idx acc)
    (remove-vertex! g acc)
    (let [res (vals @acc)]
      (info :res res)
      (when-not (some #(= #{:void} %) res)
        (->> res
             (map #(disj % :void))
             (set)
             (sort-by count)
             (#(do (info :sorted %) %))
             (reduce set/intersection)
             (map #(deref (vertex-for-id g %))))))))

(def triples
  (mapv
   #(add-triple g %)
   '[[toxi author fabric]
     [fabric type project]
     [toxi type person]]))

(def ctx (async-context {:graph g :processor eager-vertex-processor :timeout nil}))
(def toxi (register-query g 'toxi nil nil))
(def types (register-query g nil 'type nil))
(def projects (register-query g nil 'type 'project))

(execute! ctx)

;;(warn :free-ram (.freeMemory (Runtime/getRuntime)))

;; TODO add TripleGraph w/ separate sets of triple, index, rule & query vertices
