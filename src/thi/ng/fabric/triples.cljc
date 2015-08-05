(ns thi.ng.fabric.triples
  (:require
   [thi.ng.fabric.async :as f]
   [clojure.set :as set]
   [taoensso.timbre :refer [debug info warn error]]))

(defprotocol ITripleGraph
  (add-triple [_ t])
  (add-query [_ id q])
  (add-rule [_ id match production])
  (query-result [_ id]))

(defn signal-triple
  [vertex _]
  [(:id vertex) @vertex])

(defn collect-index
  [spo]
  (f/simple-collect
   (fn [val incoming]
     (info :update-index spo incoming)
     (transduce
      (map (fn [[id t]] [id (nth t spo)]))
      (completing (fn [acc [id x]] (update acc x (fnil conj #{}) id)))
      val incoming))))

(defn signal-select
  [vertex [idx sel]]
  [idx (if sel (@vertex sel [nil]) (->> @vertex vals (mapcat identity)))])

(def collect-select
  (f/simple-collect
   (fn [val incoming]
     (reduce (fn [acc [idx res]] (update acc idx (fnil into #{}) res)) val incoming))))

(defn aggregate-select
  [g]
  (f/simple-collect
   (fn [_ incoming]
     (let [res (vals (peek incoming))]
       (delay
        (when (and (== (count res) 3) (every? #(not= #{nil} %) res))
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
        (add-triple g t))
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
  (add-triple
    [_ t]
    (or (@triples t)
        (let [{:keys [subj pred obj]} indices
              v (f/add-vertex! g {:val t})]
          (f/add-edge! g v subj signal-triple nil)
          (f/add-edge! g v pred signal-triple nil)
          (f/add-edge! g v obj  signal-triple nil)
          (f/signal! v)
          (swap! triples conj t)
          v)))
  (add-query
    [_ id [s p o]]
    ;; TODO figure out way how to at to a running context
    ;; TODO remove vertices if query ID already exists
    (let [{:keys [subj pred obj]} indices
          acc (f/add-vertex! g {:val {}          ::f/collect-fn collect-select})
          res (f/add-vertex! g {:val (delay nil) ::f/collect-fn (aggregate-select g)})]
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
  (add-rule
    [_ id query production]
    (let [qid (keyword (str "inf-" (name id)))
          q   (add-query _ qid query)
          inf (f/add-vertex! g {:val #{} ::f/collect-fn (collect-inference _ production)})]
      (f/add-edge! g (:result q) inf f/signal-forward nil)
      {:query q :inf inf}))
  )

(defn triple-graph
  [g]
  (map->TripleGraph
   {:indices {:subj (index-vertex g 0)
              :pred (index-vertex g 1)
              :obj  (index-vertex g 2)}
    :triples (atom #{})
    :queries (atom {})
    :rules   (atom {})
    :g       g}))

(defn add-counter
  [g src]
  (let [v (f/add-vertex!
           g {::f/collect-fn
              (f/simple-collect
               (fn [val in]
                 (debug :updated-result (pr-str @(peek in)))
                 (count @(peek in))))})]
    (f/add-edge! g src v f/signal-forward nil)
    v))

(defn start
  []
  (let [g (triple-graph (f/compute-graph))
        toxi (:result (add-query g :toxi ['toxi nil nil]))
        types (:result (add-query g :types [nil 'type nil]))
        projects (:result (add-query g :projects [nil 'type 'project]))
        all (:result (add-query g :all [nil nil nil]))
        num-projects (add-counter g projects)
        num-types (add-counter g types)
        inf1 (add-rule g :knows [nil 'knows nil] (fn [[s p o]] [[s 'type 'person] [o 'type 'person] [o 'knows s]]))

        ctx (f/async-context {:graph g :processor f/eager-vertex-processor :timeout nil})]
    (f/execute! ctx)
    (mapv
     #(add-triple g %)
     '[[toxi author fabric]
       [fabric type project]
       [toxi type person]])
    (info :inf-vertices (get-in inf1 [:query :result :id]) (get-in inf1 [:inf :id]))
    {:g   g
     :ctx ctx
     :all all
     :toxi toxi
     :types types
     :projects projects
     :num-projects num-projects
     :num-types num-types}))
