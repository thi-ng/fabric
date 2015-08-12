(ns thi.ng.fabric.triples
  #?@(:clj
      [(:require
        [thi.ng.fabric.core :as f]
        [clojure.set :as set]
        [clojure.core.async :as a :refer [go go-loop chan close! <! >! alts! timeout]]
        [taoensso.timbre :refer [debug info warn]])]
      :cljs
      [(:require-macros
        [cljs.core.async.macros :refer [go go-loop]]
        [cljs-log.core :refer [debug info warn]])
       (:require
        [thi.ng.fabric.core :as f]
        [clojure.set :as set]
        [cljs.core.async :refer [chan close! <! >! alts! timeout]])]))

(defprotocol ITripleGraph
  (triple-indices [_])
  (add-triple! [_ t])
  (remove-triple! [_ t])
  (register-query! [_ q])
  (unregister-query! [_ q])
  (query-for-pattern [_ pattern]))

(defprotocol ITripleQuery
  (raw-pattern [_])
  (result-vertex [_]))

(defn- sole-user?
  [src user]
  (let [n (f/neighbors src)]
    (and (== 1 (count n)) (= user (first n)))))

(defn- random-id
  [] (keyword (gensym)))

(defn- signal-triple
  [vertex op] [op (:id vertex) @vertex])

(defn- collect-index
  [spo]
  (f/collect-pure
   (fn [val incoming]
     ;;(debug :update-index spo incoming)
     (debug :old-index val)
     (let [val (transduce
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
                val incoming)]
       (debug :new-index val)
       val))))

(defn- signal-index-select
  [vertex [idx sel]]
  [idx (if sel (@vertex sel [nil]) (->> @vertex vals (mapcat identity) (set)))])

(def ^:private collect-select
  (f/collect-pure
   (fn [val incoming]
     (let [val (reduce (fn [acc [idx res]] (assoc acc idx res)) val incoming)]
       ;;(debug :coll-select val incoming)
       val))))

(defn- score-collect-min-signal-vals
  [num]
  (fn [vertex]
    (if (> num (count (vals (peek (::f/uncollected @(:state vertex)))))) 0 1)))

(defn- score-collect-min-signals
  [num]
  (fn [vertex]
    (if (> num (count (::f/uncollected @(:state vertex)))) 0 1)))

(defn- aggregate-select
  [g]
  (f/collect-pure
   (fn [_ incoming]
     (let [res (vals (peek incoming))]
       ;;(debug :agg-incoming res)
       (if (every? #(not= [nil] %) res)
         (->> res
              (into #{} (map #(disj % nil)))
              (sort-by count)
              (reduce set/intersection)
              (into #{}
                    (comp (map #(f/vertex-for-id g %))
                          (filter identity)
                          (map deref))))
         #{})))))

(defn- qvar?
  "Returns true, if x is a qvar (a symbol prefixed with '?')"
  [x] (and (symbol? x) (= \? (.charAt ^String (name x) 0))))

(defn- bind-translator
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

(defn- triple-verifier
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

(defn- score-collect-join
  [vertex]
  (let [state @(:state vertex)]
    (if (and (seq (::f/uncollected state))
             (== (count (::f/signal-map @(:state vertex))) 2))
      1 0)))

(defn- collect-inference
  [g production]
  (fn [vertex]
    (let [prev @vertex
          in   (reduce into #{} (::f/uncollected @(:state vertex)))
          adds (set/difference in prev)
          inferred (mapcat production adds)]
      (debug (:id vertex) :additions adds)
      (doseq [[op t :as inf] inferred]
        (case op
          :+ (do (debug :add-triple t)
                 (add-triple! g t))
          :- (do (debug :remove-triple t)
                 (remove-triple! g t))
          (warn "invalid inference:" inf)))
      (swap! (:value vertex) set/union adds))))

(defn- index-vertex
  [g spo]
  (f/add-vertex!
   g {} {::f/collect-fn      (collect-index spo)
         ::f/score-signal-fn f/score-signal-with-new-edges}))

(def ^:private triple-vertex-spec
  {::f/score-collect-fn (constantly 0)
   ::f/score-signal-fn  f/score-signal-with-new-edges})

(declare add-query! add-query-join!)

(defrecord TripleGraph
    [g indices triples queries rules]
  f/IComputeGraph
  (add-vertex!
    [_ val vspec] (f/add-vertex! g val vspec))
  (remove-vertex!
    [_ v] (f/remove-vertex! g v))
  (vertex-for-id
    [_ id] (f/vertex-for-id g id))
  (vertices
    [_] (f/vertices g))
  (add-edge!
    [_ src dest sig opts] (f/add-edge! g src dest sig opts))
  f/IWatch
  (add-watch!
    [_ type id f] (f/add-watch! g type id f) _)
  (remove-watch!
    [_ type id] (f/remove-watch! g type id) _)
  ITripleGraph
  (triple-indices
    [_] indices)
  (add-triple!
    [_ t]
    (or (@triples t)
        (let [{:keys [subj pred obj]} indices
              v (f/add-vertex! g t triple-vertex-spec)]
          (f/add-edge! g v subj signal-triple :add)
          (f/add-edge! g v pred signal-triple :add)
          (f/add-edge! g v obj  signal-triple :add)
          (swap! triples assoc t v)
          v)))
  (remove-triple!
    [_ t]
    (if-let [v (@triples t)]
      (let [{:keys [subj pred obj]} indices]
        (f/add-edge! g v subj signal-triple :remove)
        (f/add-edge! g v pred signal-triple :remove)
        (f/add-edge! g v obj  signal-triple :remove)
        (swap! triples dissoc t)
        (f/remove-vertex! g v)
        v)
      (warn "attempting to remove unknown triple:" t)))
  (register-query!
    [_ q] (swap! queries assoc (raw-pattern q) q) q)
  (unregister-query!
    [_ q] (swap! queries dissoc (raw-pattern q) nil))
  (query-for-pattern
    [_ pattern] (@queries pattern)))

(defrecord BasicTripleQuery [id acc result pattern]
  #?@(:clj
       [clojure.lang.IDeref (deref [_] (when result @result))]
       :cljs
       [IDeref (-deref [_] (when result @result))])
  ITripleQuery
  (raw-pattern
    [_] pattern)
  (result-vertex
    [_] result)
  f/IGraphComponent
  (add-to-graph!
    [_ g]
    (let [{:keys [subj pred obj]} (triple-indices g)
          acc  (f/add-vertex!
                g {} {::f/collect-fn collect-select})
          res  (f/add-vertex!
                g nil
                {::f/collect-fn       (aggregate-select g)
                 ::f/score-collect-fn (score-collect-min-signal-vals 3)})
          [s p o] pattern
          this (assoc _ :acc acc :result res)]
      ;; TODO add index selection vertices, use existing if possible
      (f/add-edge! g subj acc signal-index-select [0 s])
      (f/add-edge! g pred acc signal-index-select [1 p])
      (f/add-edge! g obj  acc signal-index-select [2 o])
      (f/add-edge! g acc  res f/signal-forward nil)
      (register-query! g this)))
  (remove-from-graph!
    [_ g]
    (unregister-query! g _)
    (f/remove-vertex! g result)
    (f/remove-vertex! g acc)
    (assoc _ :acc nil :result nil)))

(defrecord ParametricTripleQuery
    [id sub-query result pattern]
  #?@(:clj
       [clojure.lang.IDeref (deref [_] (when result @result))]
       :cljs
       [IDeref (-deref [_] (when result @result))])
  ITripleQuery
  (raw-pattern
    [_] (mapv #(if-not (qvar? %) %) pattern))
  (result-vertex
    [_] result)
  f/IGraphComponent
  (add-to-graph!
    [_ g]
    (let [[s p o]    pattern
          vs?        (qvar? s), vp? (qvar? p), vo? (qvar? o)
          vmap       (bind-translator vs? vp? vo? s p o)
          verify     (triple-verifier s p o vs? vp? vo?)
          res-tx     (comp (map #(if (verify %) (vmap %))) (filter identity))
          collect-fn (f/collect-pure
                      (fn [_ incoming]
                        (if-let [res (seq (peek incoming))]
                          (into #{} res-tx res)
                          #{})))
          q-pattern  (raw-pattern _)
          sub-q      (or (query-for-pattern g q-pattern) (add-query! g id q-pattern))
          res        (f/add-vertex! g nil {::f/collect-fn collect-fn})]
      (f/add-edge! g (result-vertex sub-q) res f/signal-forward nil)
      (assoc _ :sub-query sub-q :result res)))
  (remove-from-graph!
    [_ g]
    (f/remove-vertex! g result)
    (when (sole-user? (result-vertex sub-query) result)
      (f/remove-from-graph! sub-query g))
    (assoc _ :sub-query nil :result nil)))

(defrecord TripleQueryJoin [id lhs rhs result]
  #?@(:clj
       [clojure.lang.IDeref (deref [_] (when result @result))]
       :cljs
       [IDeref (-deref [_] (when result @result))])
  ITripleQuery
  (raw-pattern
    [_] nil)
  (result-vertex
    [_] result)
  f/IGraphComponent
  (add-to-graph!
    [_ g]
    (let [lhs-id (:id (result-vertex lhs))
          rhs-id (:id (result-vertex rhs))
          join   (f/add-vertex!
                  g nil
                  {::f/collect-fn
                   (fn [vertex]
                     (let [sig-map (::f/signal-map @(:state vertex))
                           a (sig-map lhs-id)
                           b (sig-map rhs-id)]
                       (debug (:id vertex) :join-sets a b)
                       (reset! (:value vertex) (set/join a b))))
                   ::f/score-collect-fn
                   score-collect-join})]
      (f/add-edge! g (result-vertex lhs) join f/signal-forward nil)
      (f/add-edge! g (result-vertex rhs) join f/signal-forward nil)
      (assoc _ :result join)))
  (remove-from-graph!
    [_ g]
    (f/remove-vertex! g result)
    (when (sole-user? (result-vertex lhs) result)
      (f/remove-from-graph! lhs g))
    (when (sole-user? (result-vertex rhs) result)
      (f/remove-from-graph! rhs g))))

(defrecord TripleInferenceRule
    [id query patterns production inf]
  f/IGraphComponent
  (add-to-graph!
    [_ g]
    (let [q   (apply add-query-join! g patterns)
          inf (f/add-vertex!
               g #{} {::f/collect-fn (collect-inference g production)})]
      (f/add-edge! g (result-vertex q) inf f/signal-forward nil)
      (assoc _ :query q :inf inf))))

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

(defn add-query!
  [g id pattern]
  (f/add-to-graph!
   (map->BasicTripleQuery {:id id :pattern pattern}) g))

(defn add-param-query!
  [g id pattern]
  (f/add-to-graph!
   (map->ParametricTripleQuery {:id id :pattern pattern}) g))

(defn add-join!
  ([g lhs rhs]
   (add-join! g (random-id) lhs rhs))
  ([g id lhs rhs]
   (f/add-to-graph!
    (map->TripleQueryJoin {:id id :lhs lhs :rhs rhs}) g)))

(defn add-query-join!
  [g a b & more]
  (reduce
   (fn [acc p]
     (add-join! g (random-id) acc (add-param-query! g (random-id) p)))
   (add-join!
    g (random-id)
    (add-param-query! g (random-id) a)
    (add-param-query! g (random-id) b))
   more))

(defn add-query-filter!
  [g q flt]
  (let [tx (comp (mapcat identity) (filter flt))
        fv (f/add-vertex!
            g nil
            {::f/collect-fn
             (f/collect-pure
              (fn [_ incoming] (sequence tx incoming)))})]
    (f/add-edge! g (result-vertex q) fv f/signal-forward nil)
    fv))

(defn add-rule!
  [g id query production]
  (f/add-to-graph!
   (map->TripleInferenceRule
    {:id id :patterns query :production production})
   g))

(defn add-counter!
  [g src]
  (let [v (f/add-vertex!
           g nil
           {::f/collect-fn (f/collect-pure (fn [_ in] (count (peek in))))})]
    (f/add-edge! g src v f/signal-forward nil)
    v))

(def triple-log-transducer
  (comp
   (filter (fn [[op v]] (and (#{:add-vertex :remove-vertex} op) (vector? @v))))
   (map (fn [[op v]] [({:add-vertex :+ :remove-vertex :-} op) @v]))))

(defn add-triple-graph-logger
  [g log-fn]
  (let [ch        (chan 1024 triple-log-transducer)
        watch-id  (random-id)
        log->chan #(go (>! ch %))]
    (go-loop []
      (let [t (<! ch)]
        (when t
          (log-fn t)
          (recur))))
    (f/add-watch! g :add-vertex watch-id log->chan)
    (f/add-watch! g :remove-vertex watch-id log->chan)
    {:graph g :chan ch :watch-id watch-id}))

(defn remove-triple-graph-logger
  [{:keys [graph watch-id chan]}]
  (f/remove-watch! graph :add-vertex watch-id)
  (f/remove-watch! graph :remove-vertex watch-id)
  (close! chan))

(def inference-rules
  {:symmetric  ['[[?a ?prop ?b] [?prop type symmetric-prop]]
                (fn [{:syms [?a ?prop ?b]}] [[:+ [?b ?prop ?a]]])]
   :domain     ['[[?a ?prop nil] [?prop domain ?d]]
                (fn [{:syms [?a ?prop ?d]}] [[:+ [?a 'type ?d]]])]
   :range      ['[[nil ?prop ?a] [?prop range ?r]]
                (fn [{:syms [?a ?prop ?r]}] [[:+ [?a 'type ?r]]])]
   :transitive ['[[?a ?prop ?b] [?b ?prop ?c] [?prop type transitive-prop]]
                (fn [{:syms [?a ?prop ?c]}] [[:+ [?a ?prop ?c]]])]
   :sub-prop   ['[[?a ?prop ?b] [?prop sub-prop-of ?super]]
                (fn [{:syms [?a ?super ?b]}] [[:+ [?a ?super ?b]]])]
   :modified   ['[[?a modified ?t1] [?a modified ?t2]]
                (fn [{:syms [?a ?t1 ?t2]}] (if (not= ?t1 ?t2) [[:- [?a 'modified (min ?t1 ?t2)]]]))]})

(defn make-test-graph
  []
  (let [g (triple-graph (f/compute-graph))
        log (add-triple-graph-logger
             g #?(:clj
                  #(spit "triple-log.edn" (str (pr-str %) "\n") :append true)
                  :cljs
                  #(.log js/console (pr-str %))))
        toxi (add-query! g :toxi ['toxi nil nil])
        types (add-query! g :types [nil 'type nil])
        projects (add-query! g :projects [nil 'type 'project])
        knows (add-query! g :projects [nil 'knows nil])
        all (add-query! g :all [nil nil nil])
        num-projects (add-counter! g (result-vertex projects))
        num-types (add-counter! g (result-vertex types))
        pq (add-param-query! g :pq '[?s knows ?o])
        jq (add-query-join! g '[?p author ?prj] '[?prj type project] '[?p type person])
        tq (add-query-join! g '[?p author ?prj] '[?prj tag ?t])]
    (run! (fn [[id [q prod]]] (add-rule! g id q prod)) inference-rules)
    (run!
     #(add-triple! g %)
     '[[toxi author fabric]
       [fabric type project]
       [knows type symmetric-prop]
       [knows domain person]
       [author domain person]
       [author range creative-work]
       [parent sub-prop-of ancestor]
       [ancestor type transitive-prop]
       [ancestor domain person]
       [ancestor range person]
       [toxi modified 23]
       [toxi modified 42]])
    {:g            g
     :log          log
     :all          all
     :toxi         toxi
     :types        types
     :knows        knows
     :projects     projects
     :pq           pq
     :jq           jq
     :tq           tq
     :num-projects num-projects
     :num-types    num-types}))

(defn ^:export start-async
  []
  (let [spec     (make-test-graph)
        {:keys [g log all pq jq tq]} spec
        ctx      (f/async-execution-context {:graph g})
        ctx-chan (f/execute! ctx)]
    (go
      (let [res (<! ctx-chan)]
        (warn :result res)
        (warn (sort @all))
        (run!
         #(add-triple! g %)
         '[[toxi parent noah]
           [ingo parent toxi]
           [geom type project]
           [toxi author geom]
           [toxi knows noah]
           [geom tag clojure]
           [fabric tag clojure]])
        (f/notify! ctx)
        (let [res (<! ctx-chan)]
          (warn :result2 res)
          (warn :all (sort @all))
          (warn :pq @pq)
          (warn :jq @jq)
          (warn :tq @tq)
          (remove-triple! g '[geom tag clojure])
          (remove-triple! g '[fabric tag clojure])
          (f/notify! ctx)
          (let [res (<! ctx-chan)]
            (warn :result3 res)
            (warn :jq @jq)
            (warn :tq @tq)
            (f/stop! ctx)
            (do (remove-triple-graph-logger log)
                (warn :done))))))
    (assoc spec :ctx ctx)))


(defn ^:export start-sync
  []
  (let [spec (make-test-graph)
        {:keys [g log all pq jq tq]} spec
        ctx  (f/sync-execution-context {:graph g})
        res  (f/execute! ctx)]
    (warn :result res)
    (warn (sort @all))
    (run!
     #(add-triple! g %)
     '[[toxi parent noah]
       [ingo parent toxi]
       [geom type project]
       [toxi author geom]
       [toxi knows noah]
       [geom tag clojure]
       [fabric tag clojure]])
    (warn :result2 (f/execute! ctx))
    (warn :all (sort @all))
    (warn :pq @pq)
    (warn :jq @jq)
    (warn :tq @tq)
    (f/signal! (remove-triple! g '[geom tag clojure]) f/sync-vertex-signal)
    (f/signal! (remove-triple! g '[fabric tag clojure]) f/sync-vertex-signal)
    (warn :result3 (f/execute! ctx))
    (warn :all @all)
    (warn :jq @jq)
    (warn :tq @tq)
    (remove-triple-graph-logger log)
    (assoc spec :ctx ctx)))
