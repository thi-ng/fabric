(ns signalcollect.core)

(defn trace-v
  [pre v score]
  (prn pre (:id @v) score (dissoc @v :out :collect :score-sig :score-coll)))

(defn dump
  [g] (map (comp (juxt :id :state) deref) (vals (:vertices @g))))

(defn dot
  [g]
  (->> (:vertices @g)
       (vals)
       (mapcat
        (fn [v]
          (if (:state @v)
            (->> (:out @v)
                 (map #(str (:id @v) "->" (:target %) ";\n"))
                 (cons
                  (format
                   "%d[label=\"%d (%d)\"];\n"
                   (:id @v) (:id @v) (int (:state @v))))))))
       (apply str)
       (format "digraph g {\nranksep=2;\noverlap=scale;\n%s}")
       (spit "sc.dot")))

(defn default-score-signal
  [{:keys [state prev mod-since-sig]}]
  (if mod-since-sig
    1
    (if prev
      (if (= state prev) 0 1)
      1)))

(defn default-score-collect
  [{:keys [mod-since-collect]}]
  (if mod-since-collect 1 0))

(defn graph
  []
  (atom
   {:vertices {}
    :next-id 0}))

(defn vertex-for-id
  [g id] (get-in @g [:vertices id]))

(defn vertex
  [^long id {:keys [state collect score-sig score-coll]}]
  (atom
   {:id                id
    :state             state
    :out               #{}
    :uncollected       nil
    :collect           collect
    :mod-since-sig     false
    :mod-since-collect false
    :score-sig         (or score-sig default-score-signal)
    :score-coll        (or score-coll default-score-collect)}))

(defn edge
  [src-vertex target-vertex {:keys [signal sig-map]}]
  (let [e {:src      (:id @src-vertex)
           :target   (:id @target-vertex)
           :signal   signal
           :sig-map? sig-map}]
    (when-not ((:out @src-vertex) e)
      (swap!
       src-vertex
       #(-> %
            (update-in [:out] conj e)
            (assoc :mod-since-sig true
                   :mod-since-collect true))))
    e))

(defn add-vertex
  [g vspec]
  (let [id (:next-id @g)
        v  (vertex id vspec)]
    (swap! g #(-> % (update-in [:vertices] assoc id v) (assoc :next-id (inc id))))
    v))

(defn do-signal
  [v g]
  (let [verts (:vertices @g)]
    (swap! v assoc :prev (:state @v) :mod-since-sig false)
    (doseq [e (:out @v)]
      (swap!
       (verts (:target e))
       (fn [t]
         (if-let [sigv ((:signal e) (verts (:src e)))]
           (let [t (update-in t [:uncollected] conj sigv)]
             (if (:sig-map? e)
               (update-in t [:signals] assoc (:src e) sigv)
               t))
           t))))))

(defn do-collect
  [v]
  (swap!
   v #(reduce
       (fn [v sig] ((:collect v) v sig))
       (assoc % :uncollected nil :mod-since-collect false)
       (:uncollected %))))

(defn execute-scored
  [g n sig-thresh coll-thresh]
  (loop [done false, i 0]
    (when (and (not done) (< i n))
      (let [done (reduce
                  (fn [done v]
                    (let [v' @v
                          score ((:score-sig v') v')
                          done (if (> score sig-thresh)
                                 (do (do-signal v g) false)
                                 done)]
                      ;;(trace-v :sig v score)
                      done))
                  true (vals (:vertices @g)))
            ;;_ (prn :signal-done done)
            done (reduce
                  (fn [done v]
                    (let [v' @v
                          score ((:score-coll v') v')
                          ;;_ (trace-v :coll-1 v score)
                          done (if (> score coll-thresh)
                                 (do (do-collect v) false)
                                 done)]
                      ;;(trace-v :coll-2 v score)
                      done))
                  done (vals (:vertices @g)))]
        ;;(prn :collect-done done i (dump g))
        ;;(prn "-----")
        (recur done (inc i))))))

(defn signal-sssp
  [v] (if (:state @v) (inc (:state @v))))

(defn collect-sssp
  [v sig]
  (if (:state v)
    (assoc v :state (min (:state v) sig))
    (assoc v :state sig)))

(defn score-sig-sssp
  [{:keys [state prev]}]
  (if (and state (or (not prev) (not= state prev))) 1 0))

(defn score-coll-sssp
  [v] (count (:uncollected v)))

;; a -> b -> c ; b -> d
(defn sssp-test-graph
  []
  (let [g (graph)
        spec {:collect collect-sssp :score-sig score-sig-sssp :score-coll score-coll-sssp}
        a (add-vertex g (assoc spec :state 0))
        [b c d e f] (repeatedly 5 #(add-vertex g spec))]
    (doseq [[a b] [[a b] [b c] [c d] [a e] [d f] [e f]]]
      (edge a b {:signal signal-sssp :sig-map true}))
    g))

(defn make-strand
  [verts]
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
  [n ne]
  (let [g (graph)
        spec {:collect collect-sssp :score-sig score-sig-sssp :score-coll score-coll-sssp}
        verts (->> (range n)
                   (map (fn [_] (add-vertex g spec)))
                   (cons (add-vertex g (assoc spec :state 0)))
                   vec)]
    (dotimes [i ne]
      (->> (make-strand verts)
           (partition 2 1)
           (map (fn [[a b]] (edge (verts a) (verts b) {:signal signal-sssp})))
           (doall)))
    g))
