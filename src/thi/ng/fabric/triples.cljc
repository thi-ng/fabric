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
  (add-triple! [_ t])
  (remove-triple! [_ t])
  (add-query! [_ id q])
  (add-rule! [_ id match production])
  (query-result [_ id]))

(defn signal-triple
  [vertex op] [op (:id vertex) @vertex])

(defn collect-index
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

(defn signal-index-select
  [vertex [idx sel]]
  [idx (if sel (@vertex sel [nil]) (->> @vertex vals (mapcat identity) (set)))])

(def collect-select
  (f/collect-pure
   (fn [val incoming]
     (let [val (reduce (fn [acc [idx res]] (assoc acc idx res)) val incoming)]
       ;;(debug :coll-select val incoming)
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

(defn add-param-query!
  [g id [s p o]]
  (let [vs?        (qvar? s), vp? (qvar? p), vo? (qvar? o)
        vmap       (bind-translator vs? vp? vo? s p o)
        verify     (triple-verifier s p o vs? vp? vo?)
        res-tx     (comp (map #(if (verify %) (vmap %))) (filter identity))
        collect-fn (f/collect-pure
                    (fn [_ incoming]
                      (if-let [res (seq (peek incoming))]
                        (into #{} res-tx res)
                        #{})))
        q-pattern  [(if vs? nil s) (if vp? nil p) (if vo? nil o)]
        q-spec     (add-query! g id q-pattern)
        resv       (f/add-vertex! g nil {::f/collect-fn collect-fn})]
    (f/add-edge! g (:result q-spec) resv f/signal-forward nil)
    (assoc q-spec :qvar-result resv)))

(defn score-collect-join
  [vertex]
  (let [state @(:state vertex)]
    (if (and (seq (::f/uncollected state)) (== (count (::f/signal-map @(:state vertex))) 2)) 1 0)))

(defn add-join!
  [g qa qb]
  (let [jv (f/add-vertex!
            g nil ;; FIXME delay
            {::f/collect-fn
             (fn [vertex]
               (let [sig-map (::f/signal-map @(:state vertex))
                     a (sig-map (:id (:qvar-result qa)))
                     b (sig-map (:id (:qvar-result qb)))]
                 (debug (:id vertex) :join-sets a b)
                 (reset! (:value vertex) (set/join a b))))
             ::f/score-collect-fn
             score-collect-join})]
    (f/add-edge! g (:qvar-result qa) jv f/signal-forward nil)
    (f/add-edge! g (:qvar-result qb) jv f/signal-forward nil)
    {:a qa :b qb :qvar-result jv}))

(defn add-join-query!
  [g [a b & more]]
  (if b
    (reduce
     (fn [acc p] (add-join! g acc (add-param-query! g (keyword (gensym)) p)))
     (add-join! g
                (add-param-query! g (keyword (gensym)) a)
                (add-param-query! g (keyword (gensym)) b))
     more)
    (add-param-query! g (keyword (gensym)) a)))

(defn add-query-filter!
  [g q-vertex flt]
  (let [tx (comp (mapcat identity) (filter flt))
        fv (f/add-vertex!
            g nil
            {::f/collect-fn
             (f/collect-pure
              (fn [_ incoming] (sequence tx incoming)))})]
    (f/add-edge! g q-vertex fv f/signal-forward nil)
    fv))

(defn collect-inference
  [g production]
  (fn [vertex]
    (let [prev @vertex
          in   (reduce into #{} (::f/uncollected @(:state vertex)))
          adds (set/difference in prev)
          inferred (mapcat production adds)]
      (debug :additions adds)
      (doseq [t inferred]
        (debug :add-triple t)
        (add-triple! g t))
      (swap! (:value vertex) set/union in (set inferred)))))

(defn- index-vertex
  [g spo]
  (f/add-vertex!
   g {} {::f/collect-fn      (collect-index spo)
         ::f/score-signal-fn f/score-signal-with-new-edges}))

(def triple-vertex-spec
  {::f/score-collect-fn (constantly 0)
   ::f/score-signal-fn  f/score-signal-with-new-edges})

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
  (add-query!
    [_ id [s p o]]
    ;; TODO remove vertices if query ID already exists
    ;; TODO add default [nil nil nil] query & re-use
    (let [{:keys [subj pred obj]} indices
          acc  (f/add-vertex!
                g {} {::f/collect-fn collect-select})
          res  (f/add-vertex!
                g nil
                {::f/collect-fn       (aggregate-select g)
                 ::f/score-collect-fn (score-collect-min-signal-vals 3)})
          spec {:pattern [s p o] :acc acc :result res}]
      (f/add-edge! g subj acc signal-index-select [0 s])
      (f/add-edge! g pred acc signal-index-select [1 p])
      (f/add-edge! g obj  acc signal-index-select [2 o])
      (f/add-edge! g acc  res f/signal-forward nil)
      (swap! queries assoc id spec)
      spec))
  (query-result
    [_ id] (when-let [q (@queries id)] @(:result q)))
  (add-rule!
    [_ id query production]
    (let [q   (add-join-query! _ query)
          inf (f/add-vertex!
               g #{} {::f/collect-fn (collect-inference _ production)})]
      (f/add-edge! g (:qvar-result q) inf f/signal-forward nil)
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
        watch-id  (keyword (gensym))
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

(defn make-test-graph
  []
  (let [g (triple-graph (f/compute-graph))
        log (add-triple-graph-logger
             g #?(:clj
                  #(spit "triple-log.edn" (str (pr-str %) "\n") :append true)
                  :cljs
                  #(.log js/console (pr-str %))))
        toxi (:result (add-query! g :toxi ['toxi nil nil]))
        types (:result (add-query! g :types [nil 'type nil]))
        projects (:result (add-query! g :projects [nil 'type 'project]))
        knows (:result (add-query! g :projects [nil 'knows nil]))
        all (:result (add-query! g :all [nil nil nil]))
        num-projects (add-counter! g projects)
        num-types (add-counter! g types)
        _ (add-rule!
           g :symmetric '[[?a ?prop ?b] [?prop type symmetric-prop]]
           (fn [{:syms [?a ?prop ?b]}] [[?b ?prop ?a]]))
        _ (add-rule!
           g :domain '[[?a ?prop nil] [?prop domain ?d]]
           (fn [{:syms [?a ?prop ?d]}] [[?a 'type ?d]]))
        _ (add-rule!
           g :range '[[nil ?prop ?a] [?prop range ?r]]
           (fn [{:syms [?a ?prop ?r]}] [[?a 'type ?r]]))
        _ (add-rule!
           g :transitive '[[?a ?prop ?b] [?b ?prop ?c] [?prop type transitive-prop]]
           (fn [{:syms [?a ?prop ?c]}] [[?a ?prop ?c]]))
        _ (add-rule!
           g :sub-prop '[[?a ?prop ?b] [?prop sub-prop-of ?super]]
           (fn [{:syms [?a ?super ?b]}] [[?a ?super ?b]]))
        pq (:qvar-result (add-param-query! g :pq '[?s knows ?o]))
        jq (:qvar-result (add-join-query! g '[[?p author ?prj] [?prj type project] [?p type person]]))
        tq (:qvar-result (add-join-query! g '[[?p author ?prj] [?prj tag ?t]]))]
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
       [ancestor range person]])
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
        (let [res (<! ctx-chan)]
          (warn :result res)
          (warn :all (sort @all))
          (warn :pq @pq)
          (warn :jq @jq)
          (warn :tq @tq)
          (remove-triple! g '[geom tag clojure])
          (remove-triple! g '[fabric tag clojure])
          (let [res (<! ctx-chan)]
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
