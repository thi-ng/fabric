(ns thi.ng.fabric.async-foo)

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
              (debug "signal fn for" (:id v) "returned nil, skipping...")))
          (recur (next outs)))))))

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

(def default-async-context-opts
  {:collect-thresh 0
   :signal-thresh  0
   :processor      eager-async-vertex-processor
   :bus-size       16
   :timeout        25
   :max-ops        1e6
   :threads        #?(:clj (inc (.availableProcessors (Runtime/getRuntime))) :cljs 1)})

(defn stop-async-execution
  [bus vertices]
  (warn "stopping execution context...")
  (run! close-input! vertices)
  (run! async/close! bus))

(defn async-execution-context
  [opts]
  (let [ctx       (merge default-async-context-opts opts)
        bus       (vec (repeatedly (:bus-size ctx) async/chan))
        ctx       (assoc ctx :bus bus :result (or (:result ctx) (async/chan)))
        g         (:graph ctx)
        processor (:processor ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        max-t     (:timeout ctx)
        res-chan  (:result ctx)
        watch-id  (keyword (gensym))]
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
        (stop-async-execution bus (vertices g))
        (remove-watch! g :add-vertex watch-id)
        (remove-watch! g :remove-vertex watch-id)
        (remove-watch! g :add-edge watch-id)
        res-chan)
      (notify! [_ evt]
        (go (>! (rand-nth bus) evt)))
      (include-vertex!
        [_ v]
        (info "including vertex into async context" (:id v) @v)
        (processor v _)
        (when (should-signal? v s-thresh)
          (signal! v async-vertex-signal))
        #_(when (should-collect? v c-thresh)
            (collect! v))
        _)
      (execute!
        [_]
        (let [t0 (now)]
          (add-watch! g :add-vertex watch-id
                      (fn [[__ v]]
                        (info :ctx-add-vertex (:id v) @v)
                        (processor v _)))
          (add-watch! g :remove-vertex watch-id
                      (fn [[__ v]] (signal! v async-vertex-signal)))
          (add-watch! g :add-edge watch-id
                      (fn [[__ src dest]]
                        (info :ctx-add-edge (:id src) (:id dest))
                        (signal! src async-vertex-signal)))
          (go
            (run! #(include-vertex! _ %) (vertices g))
            (loop [colls 0, sigs 0]
              (if (<= (+ colls sigs) max-ops)
                (let [t (if max-t (async/timeout max-t))
                      [[evt v ex] port] (async/alts! (if max-t (conj bus t) bus))]
                  (if (= port t)
                    (if (:auto-stop ctx)
                      (do (stop! _)
                          (async-execution-result :converged res-chan colls sigs t0))
                      (do (async-execution-result :converged res-chan colls sigs t0)
                          (recur colls sigs)))
                    (case evt
                      :collect (recur (inc colls) sigs)
                      :signal  (recur colls (inc sigs))
                      :error   (do (warn ex "@ vertex" v)
                                   (when (:auto-stop ctx) (stop! _))
                                   (async-execution-result
                                    :error res-chan colls sigs t0
                                    {:reason-event     evt
                                     :reason-exception ex
                                     :reason-vertex    v}))
                      (do (warn "execution interrupted")
                          (async-execution-result
                           :stopped res-chan colls sigs t0)))))
                (do (stop! _)
                    (async-execution-result :max-ops-reached res-chan colls sigs t0)))))
          res-chan)))))

(defn ^java.util.TimerTask timer-task [^java.util.Timer t f]
  (proxy [java.util.TimerTask] []
    (run [] (.cancel t) (f))))

(defn dynamic-timeout
  [interval]
  (let [ctrl    (async/chan (async/sliding-buffer 1))
        out     (async/chan)
        timer   (volatile! nil)
        trigger #(go (warn :trigger-timeout) (>! out [:timeout]))]
    (go-loop []
      (if (<! ctrl)
        (do (when @timer (.cancel ^java.util.Timer @timer))
            (vreset! timer (doto (java.util.Timer.) (.schedule (timer-task trigger) (long interval))))
            (recur))
        (do (when @timer (.cancel ^java.util.Timer @timer))
            #_(async/close! out))))
    [ctrl out]))

(defn channel-mix
  [inputs out]
  (go-loop []
    (let [[x] (async/alts! inputs)]
      (warn :mix x)
      (if x
        (do (>! out x)
            (recur))
        (do (run! async/close! inputs)
            (async/close! out)
            (warn :mix-closed)))))
  out)

(defn eager-async-vertex-processor2
  [{:keys [id] :as vertex} ctx]
  (let [{:keys [collect-thresh signal-thresh]} @ctx
        in (input-channel vertex)]
    (go-loop []
      (let [[src-id sig] (<! in)]
        (if sig
          (do (debug id "receive from" src-id ":" (pr-str sig))
              (receive-signal vertex src-id sig)
              (when (should-collect? vertex collect-thresh)
                (collect! vertex)
                (notify! ctx [:collect vertex])
                (when (should-signal? vertex signal-thresh)
                  (signal! vertex async-vertex-signal)
                  (notify! ctx [:signal vertex])))
              (recur))
          (warn id " stopped")
          )))
    vertex))

(defn async-execution-context2*
  [opts]
  (let [ctx       (merge default-async-context-opts opts)
        ;;bus       (vec (repeatedly (:bus-size ctx) async/chan))
        ctx       (assoc ctx
                         ;;:bus bus
                         :result (or (:result ctx) (async/chan (async/dropping-buffer 1))))
        g         (:graph ctx)
        processor eager-async-vertex-processor2 ;; (:processor ctx)
        c-thresh  (:collect-thresh ctx)
        s-thresh  (:signal-thresh ctx)
        max-ops   (:max-ops ctx)
        res-chan  (:result ctx)
        watch-id  (keyword (gensym))
        sigs      (atom 0)
        colls     (atom 0)
        active    (atom #{})
        result?   (atom false)
        ctrl      (async/chan)]
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
        (stop-async-execution nil (vertices g))
        (async/close! ctrl)
        (remove-watch! g :add-vertex watch-id)
        (remove-watch! g :remove-vertex watch-id)
        (remove-watch! g :add-edge watch-id)
        res-chan)
      (notify! [_ [evt v ex]]
        (warn :notify evt (:id v))
        (case evt
          :signal  (do (swap! sigs inc)
                       (if (should-collect? v c-thresh)
                         (do
                           (warn :activate (:id v))
                           (swap! active conj (:id v)))
                         (do
                           (warn :deactivate (:id v))
                           (swap! active disj (:id v)))))
          :collect (do (swap! colls inc)
                       (if (should-signal? v s-thresh)
                         (do
                           (warn :activate (:id v))
                           (swap! active conj (:id v)))
                         (do
                           (warn :deactivate (:id v))
                           (swap! active disj (:id v)))))
          (warn :unknown-notify evt))
        (warn :active-count (count @active) :sigs @sigs :colls @colls))
      (include-vertex!
        [_ v]
        (info "including vertex into async context" (:id v) @v)
        (processor v _)
        (when (should-signal? v s-thresh)
          (signal! v async-vertex-signal))
        _)
      (execute!
        [_]
        (let [t0 (now)]
          (add-watch!
           g :add-vertex watch-id
           (fn [[__ v]]
             (debug :ctx-add-vertex (:id v) @v)
             (processor v _)))
          (add-watch!
           g :remove-vertex watch-id
           (fn [[__ v]]
             (signal! v async-vertex-signal)))
          (add-watch!
           g :add-edge watch-id
           (fn [[__ src dest]]
             (debug :ctx-add-edge (:id src) (:id dest))
             (signal! src async-vertex-signal)))
          (reset! active
                  (into #{}
                        (comp
                         (filter #(or (should-signal? % s-thresh) (should-collect? % c-thresh)))
                         (map :id))
                        (vertices g)))
          (run! #(include-vertex! _ %) (vertices g))
          res-chan)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#_(dotimes [i (:parallel ctx)]
    (let [mix (channel-mix (conj bus t-out) (async/chan))]
      (go-loop []
        (prn :sc @sigs @colls)
        (if (<= (+ @sigs @colls) max-ops)
          (let [[evt v ex] (<! mix)]
            (prn (str "thread " i "event: " evt ": " v))
            (case evt
              :timeout (if (:auto-stop ctx)
                         (when-not @result?
                           (reset! result? true)
                           (stop! _)
                           (async-execution-result :converged res-chan @colls @sigs t0))
                         (do (>! t-ctrl :reset)
                             (async-execution-result :converged res-chan @colls @sigs t0)
                             (recur)))
              :signal  (do (>! t-ctrl :reset) (swap! sigs inc) (recur))
              :collect (do (>! t-ctrl :reset) (swap! colls inc) (recur))
              :error   (do (warn ex "@ vertex" v)
                           (reset! result? true)
                           (stop! _)
                           (async-execution-result
                            :error res-chan @colls @sigs t0
                            {:reason-event     evt
                             :reason-exception ex
                             :reason-vertex    v}))
              (when-not @result?
                (reset! result? true)
                (warn "execution interrupted")
                (async-execution-result
                 :stopped res-chan @colls @sigs t0))))
          (when-not @result?
            (reset! result? true)
            (stop! _)
            (async-execution-result :max-ops-reached res-chan @colls @sigs t0))))))
