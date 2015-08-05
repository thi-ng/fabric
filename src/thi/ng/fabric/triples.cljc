(ns thi.ng.fabric.triples
  (:require
   [thi.ng.fabric.async :as f]
   [clojure.set :as set]
   [taoensso.timbre :refer [debug info warn error]]))

(defprotocol ITripleGraph
  (add-triple! [_ t])
  (remove-triple! [_ t])
  (add-query! [_ id q])
  (add-rule! [_ id match production])
  (query-result [_ id]))

(defn signal-triple
  [vertex op]
  [op (:id vertex) @vertex])

(defn collect-index
  [spo]
  (f/simple-collect
   (fn [val incoming]
     ;;(info :update-index spo incoming)
     (transduce
      (map (fn [[op id t]] [op id (nth t spo)]))
      (completing
       (fn [acc [op id x]]
         (case op
           :add    (update acc x (fnil conj #{}) id)
           :remove (if-let [idx (acc x)]
                     (if (= #{id} idx)
                       (dissoc acc x)
                       (update acc x disj id))
                     acc)
           (do (warn "ignoring unknown index signal op:" op)
               acc))))
      val incoming))))

(defn signal-select
  [vertex [idx sel]]
  [idx (if sel (@vertex sel [nil]) (->> @vertex vals (mapcat identity) (set)))])

(def collect-select
  (f/simple-collect
   (fn [val incoming]
     (let [val (reduce (fn [acc [idx res]] (assoc acc idx res)) val incoming)]
       ;;(info :coll-select val incoming)
       val))))

(defn score-collect-min-signal-vals
  [num]
  (fn [vertex]
    (if (> num (count (vals (peek (::f/uncollected @(:state vertex)))))) 0 1)))

(defn score-collect-min-signals
  [num]
  (fn [vertex]
    (if (> num (count (::f/uncollected @(:state vertex)))) 0 1)))

(defn aggregate-select
  [g]
  (f/simple-collect
   (fn [_ incoming]
     (let [res (vals (peek incoming))]
       ;;(info :agg-incoming res)
       (delay
        (when (every? #(not= [nil] %) res)
          (->> res
               (map #(disj % nil))
               (set)
               (sort-by count)
               (reduce set/intersection)
               (map #(deref (f/vertex-for-id g %)))
               (set))))))))

(defn collect-inference
  [g production]
  (fn [vertex]
    (let [prev @vertex
          in   (transduce
                (map deref)
                (completing into)
                #{}
                (::f/uncollected @(:state vertex)))
          adds (set/difference in prev)
          inferred (mapcat production adds)]
      (debug :additions adds)
      (doseq [t inferred]
        (info :add-triple t)
        (add-triple! g t))
      (swap! (:state vertex) update :val set/union in (set inferred)))))

(defn- index-vertex
  [g spo]
  (f/add-vertex!
   g {:val                {}
      ::f/collect-fn      (collect-index spo)
      ::f/score-signal-fn (constantly 1)}))

(defrecord TripleGraph
    [g indices triples queries rules]
  f/IComputeGraph
  (add-vertex!
    [_ v] (f/add-vertex! g v))
  (remove-vertex!
    [_ v] (f/remove-vertex! g v))
  (vertex-for-id
    [_ id] (f/vertex-for-id g id))
  (vertices
    [_] (f/vertices g))
  (add-edge!
    [_ src dest sig opts] (f/add-edge! g src dest sig opts))
  ITripleGraph
  (add-triple!
    [_ t]
    (or (@triples t)
        (let [{:keys [subj pred obj]} indices
              v (f/add-vertex! g {:val t})]
          (f/add-edge! g v subj signal-triple :add)
          (f/add-edge! g v pred signal-triple :add)
          (f/add-edge! g v obj  signal-triple :add)
          (f/signal! v)
          (swap! triples assoc t v)
          v)))
  (remove-triple!
    [_ t]
    (if-let [v (@triples t)]
      (let [{:keys [subj pred obj]} indices]
        (f/add-edge! g v subj signal-triple :remove)
        (f/add-edge! g v pred signal-triple :remove)
        (f/add-edge! g v obj  signal-triple :remove)
        (f/signal! v)
        (swap! triples dissoc t)
        v)
      (warn "attempting to remove unknown triple:" t)))
  (add-query!
    [_ id [s p o]]
    ;; TODO figure out way how to at to a running context
    ;; TODO remove vertices if query ID already exists
    (let [{:keys [subj pred obj]} indices
          acc (f/add-vertex!
               g {:val {}
                  ::f/collect-fn collect-select})
          res (f/add-vertex!
               g {:val (delay nil)
                  ::f/collect-fn (aggregate-select g)
                  ::f/score-collect-fn (score-collect-min-signal-vals 3)})]
      (f/add-edge! g subj acc signal-select [0 s])
      (f/add-edge! g pred acc signal-select [1 p])
      (f/add-edge! g obj  acc signal-select [2 o])
      (f/add-edge! g acc  res f/signal-forward nil)
      (f/signal! subj)
      (f/signal! pred)
      (f/signal! obj)
      (swap! queries assoc id {:acc acc :result res})
      {:acc acc :result res}))
  (query-result
    [_ id] (when-let [q (@queries id)] @@(:res q)))
  (add-rule!
    [_ id query production]
    (let [qid (keyword (str "inf-" (name id)))
          q   (add-query! _ qid query)
          inf (f/add-vertex!
               g {:val #{}
                  ::f/collect-fn (collect-inference _ production)})]
      (f/add-edge! g (:result q) inf f/signal-forward nil)
      {:query q :inf inf}))
  )

(defn triple-graph
  [g]
  (map->TripleGraph
   {:indices {:subj (index-vertex g 0)
              :pred (index-vertex g 1)
              :obj  (index-vertex g 2)}
    :triples (atom {})
    :queries (atom {})
    :rules   (atom {})
    :g       g}))

(defn add-counter!
  [g src]
  (let [v (f/add-vertex!
           g {::f/collect-fn
              (f/simple-collect
               (fn [val in]
                 (debug :updated-result (pr-str @(peek in)))
                 (count @(peek in))))})]
    (f/add-edge! g src v f/signal-forward nil)
    v))

(defn qvar?
  "Returns true, if x is a qvar (a symbol prefixed with '?')"
  [x] (and (symbol? x) (= \? (.charAt ^String (name x) 0))))

(defn bind-translator
  [vs? vp? vo? s p o]
  (if vs?
    (if vp?
      (if vo?
        (fn [r] {s (r 0) p (r 1) o (r 2)})
        (fn [r] {s (r 0) p (r 1)}))
      (if vo?
        (fn [r] {s (r 0) o (r 2)})
        (fn [r] {s (r 0)})))
    (if vp?
      (if vo?
        (fn [r] {p (r 1) o (r 2)})
        (fn [r] {p (r 1)}))
      (if vo?
        (fn [r] {o (r 2)})
        (fn [_] {})))))

(defn pattern-translator
  [vs? vp? vo? [s p o]]
  (if (qvar? vs? s)
    (if (qvar? vp? p)
      (if (qvar? vo? o)
        (fn [p] [nil nil nil])
        (fn [p] [nil nil (p 2)]))
      (if (qvar? vo? o)
        (fn [p] [nil (p 1) nil])
        (fn [p] (assoc p 0 nil))))
    (if (qvar? vp? p)
      (if (qvar? vo? o)
        (fn [p] [(p 0) nil nil])
        (fn [p] (assoc p 1 nil)))
      (if (qvar? vo? o)
        (fn [p] (assoc p 2 nil))
        identity))))

(defn triple-verifier
  [ts tp to vars varp varo]
  (cond
    (and vars varp varo) (cond
                           (= ts tp to) (fn [r] (= (r 0) (r 1) (r 2)))
                           (= ts tp) (fn [r] (and (= (r 0) (r 1)) (not= (r 0) (r 2))))
                           (= ts to) (fn [r] (and (= (r 0) (r 2)) (not= (r 0) (r 1))))
                           (= tp to) (fn [r] (and (= (r 1) (r 2)) (not= (r 0) (r 1))))
                           :else (constantly true))
    (and vars varp)      (if (= ts tp)
                           (fn [r] (= (r 0) (r 1)))
                           (fn [r] (not= (r 0) (r 1))))
    (and vars varo)      (if (= ts to)
                           (fn [r] (= (r 0) (r 2)))
                           (fn [r] (not= (r 0) (r 2))))
    (and varp varo)      (if (= tp to)
                           (fn [r] (= (r 1) (r 2)))
                           (fn [r] (not= (r 1) (r 2))))
    :else                (constantly true)))

(defn param-query
  [g id [s p o]]
  (let [vs?        (qvar? s), vp? (qvar? p), vo? (qvar? o)
        vmap       (bind-translator vs? vp? vo? s p o)
        verify     (triple-verifier s p o vs? vp? vo?)
        collect-fn (f/simple-collect
                    (fn [_ incoming]
                      (delay
                       (when-let [res (peek incoming)]
                         (into #{}
                               (comp (map #(if (verify %) (vmap %)))
                                     (filter identity))
                               @res)))))
        q-pattern  [(if vs? nil s) (if vp? nil p) (if vo? nil o)]
        q-spec     (add-query! g id q-pattern)
        r-vertex   (f/add-vertex! g {:val (delay nil) ::f/collect-fn collect-fn})]
    (f/add-edge! g (:result q-spec) r-vertex f/signal-forward nil)
    (assoc q-spec :qvar-result r-vertex)))

(defn add-join-query
  [g pat-a pat-b]
  (let [qa  (param-query g (keyword (gensym)) pat-a)
        qb  (param-query g (keyword (gensym)) pat-b)
        jfn (fn [vertex]
              (let [state (:state vertex)
                    a @(get-in @state [::f/signal-map (:id (:qvar-result qa))])
                    b @(get-in @state [::f/signal-map (:id (:qvar-result qb))])]
                (info :join-sets a b)
                (swap! state assoc :val (set/join a b))))
        jv  (f/add-vertex!
             g {:val (delay nil)
                ::f/collect-fn jfn
                ::f/score-collect-fn #(if (== (count (::f/signal-map @(:state %))) 2) 1 0)})]
    (f/add-edge! g (:qvar-result qa) jv f/signal-forward nil)
    (f/add-edge! g (:qvar-result qb) jv f/signal-forward nil)
    {:a qa :b qb :join jv}))

(defn start
  []
  (let [g (triple-graph (f/compute-graph))
        toxi (:result (add-query! g :toxi ['toxi nil nil]))
        types (:result (add-query! g :types [nil 'type nil]))
        projects (:result (add-query! g :projects [nil 'type 'project]))
        knows (:result (add-query! g :projects [nil 'knows nil]))
        all (:result (add-query! g :all [nil nil nil]))
        num-projects (add-counter! g projects)
        num-types (add-counter! g types)
        inf1 (add-rule! g :knows [nil 'knows nil] (fn [[s p o]] [[s 'type 'person] [o 'type 'person] [o 'knows s]]))
        pq (:qvar-result (param-query g :pq '[?s knows ?o]))
        jq (:join (add-join-query g '[?p author ?prj] '[?prj type project]))
        ctx (f/async-context {:graph g :processor f/eager-vertex-processor :timeout nil})]
    (f/execute! ctx)
    (mapv
     #(add-triple! g %)
     '[[toxi author fabric]
       [fabric type project]
       [toxi type person]])
    (info :inf-vertices (get-in inf1 [:query :result :id]) (get-in inf1 [:inf :id]))
    {:g   g
     :ctx ctx
     :all all
     :toxi toxi
     :types types
     :knows knows
     :projects projects
     :pq pq
     :jq jq
     :num-projects num-projects
     :num-types num-types}))
