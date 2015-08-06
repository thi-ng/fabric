(ns thi.ng.fabric.core
  #?@(:clj
      [(:require
        [taoensso.timbre :refer [debug info warn]]
        [clojure.core.async :refer [go go-loop chan close! <! >! alts! timeout]])]
      :cljs
      [(:require-macros
        [cljs.core.async.macros :refer [go go-loop]]
        [cljs-log.core :refer [debug info warn]])
       (:require
        [cljs.core.async :as a :refer [chan close! <! >! alts! timeout]])]))

;; #?(:clj (taoensso.timbre/set-level! :warn))

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

(defn collect-pure
  [collect-fn]
  (fn [vertex]
    (swap! (:state vertex) #(update % :val collect-fn (::uncollected %)))))

(def default-collect (collect-pure into))

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
  #?@(:clj
       [clojure.lang.IDeref
        (deref
         [_] (:val @state))]
       :cljs
       [IDeref
        (-deref
         [_] (:val @state))])
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
    (let [state @state]
      (reset! last-sig-state (:val state))
      (go-loop [outs @outs]
        (let [[v [f opts]] (first outs)]
          (when v
            (let [signal (f _ opts)]
              (if-not (nil? signal)
                (>! (input-channel v) [id signal])
                (warn "signal fn for" (:id v) "returned nil, skipping...")))
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

(defrecord InMemoryGraph [state]
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

(defrecord LoggedGraph [g log-chan]
  IComputeGraph
  (add-vertex! [_ v]
    (let [v (add-vertex! g v)]
      (go (>! log-chan [:add-vertex (:id v) @v]))
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

(defn eager-vertex-processor
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
                  (signal! vertex)))
              (recur))
          ;;(debug id " stopped")
          )))
    vertex))

(defn- now [] #?(:clj (System/nanoTime) :cljs (.getTime (js/Date.))))

(defn execution-result
  [type out colls sigs t0 & [opts]]
  (go
    (>! out
        (->> {:collections colls
              :signals     sigs
              :type        type
              :runtime     #?(:clj (* (- (now) t0) 1e-6) :cljs (- (now) t0))}
             (merge opts)))))

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

(defn execution-context
  [opts]
  (let [ctx       (merge default-context-opts opts)
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
      (execute!
        [_]
        (let [t0 (now)]
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
                           {:reason        :terminated
                            :reason-event  evt
                            :reason-vertex v})))))
                (execution-result :max-ops-reached result colls sigs t0))))
          result)))))
