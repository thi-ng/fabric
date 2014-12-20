(ns signalcollect.core)

(defn trace-v
  [pre v score]
  #_(prn pre (:id @v) score (dissoc @v :out :collect :score-sig :score-coll)))

(defn dump
  [g]
  (->> (:vertices @g)
       (vals)
       (map deref)
       (sort-by :id)
       (map (juxt :id :state))))

(defn dot
  [g]
  (->> (:vertices @g)
       (vals)
       (mapcat
        (fn [v]
          (if (:state @v)
            (->> (:out @v)
                 (map #(str (:id @v) "->" (:target %) "[label=" (:weight %) "];\n"))
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
  [v]
  (+ (count (:uncollected v)) (if (:mod-since-collect v) 1 0)))

(defn signal-forward [e v] (:state v))

(defn collect-union
  [v sig] (update-in v [:state] into sig))

(defn graph
  []
  (atom
   {:vertices {}
    :next-id 0}))

(defn vertex-for-id
  [g id] (get-in @g [:vertices id]))

(defrecord Vertex
    [id state prev
     out uncollected
     collect score-sig score-coll
     mod-sig mod-collect])

(defrecord Edge
    [src target signal weight sig-map?])

(defn vertex
  [^long id {:keys [state collect score-sig score-coll]}]
  (atom
   (Vertex.
    id state nil
    #{} nil
    collect
    (or score-sig default-score-signal)
    (or score-coll default-score-collect)
    false
    false)))

(defn edge
  [src-vertex target-vertex {:keys [weight signal sig-map]}]
  (let [e (Edge. (:id @src-vertex) (:id @target-vertex) signal (or weight 1) sig-map)]
    (when-not ((:out @src-vertex) e)
      (swap!
       src-vertex
       #(-> %
            (update-in [:out] conj e)
            (assoc :mod-sig true :mod-collect true))))
    e))

(defn add-vertex
  [g vspec]
  (let [id (:next-id @g)
        v  (vertex id vspec)]
    (swap! g #(-> % (update-in [:vertices] assoc id v) (assoc :next-id (inc id))))
    v))

(defn do-signal
  [v g]
  (let [verts (:vertices @g)
        v' @v]
    (swap! v assoc :prev (:state v') :mod-sig false)
    (doseq [{:keys [src] :as e} (:out v')]
      (swap!
       (verts (:target e))
       (fn [t]
         (if-let [sigv ((:signal e) e v')]
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
       (assoc % :uncollected nil :mod-collect false)
       (:uncollected %))))

(defn execute-scored-sync
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
                      (trace-v :sig v score)
                      done))
                  true (vals (:vertices @g)))
            ;;_ (prn :signal-done done)
            done (reduce
                  (fn [done v]
                    (let [v' @v
                          score ((:score-coll v') v')
                          _ (trace-v :coll-1 v score)
                          done (if (> score coll-thresh)
                                 (do (do-collect v) false)
                                 done)]
                      (trace-v :coll-2 v score)
                      done))
                  done (vals (:vertices @g)))]
        ;;(prn :collect-done done i (dump g))
        ;;(prn "-----")
        (recur done (inc i))))))
