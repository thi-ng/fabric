(ns thi.ng.fabric.utils)

(defn trace-v
  [pre v score]
  (prn pre (:id @v) score (dissoc @v :out :collect :score-sig :score-coll)))

(defn sorted-vertex-values
  [vertices]
  (->> vertices
       (sort-by :id)
       (map (juxt :id deref))))

#?(:clj
   (defn vertices->dot
     [path vertices flt]
     (->> vertices
          (filter flt)
          (sort-by :id)
          (mapcat
           (fn [v]
             (if-let [outs @(:outs v)]
               (let [val @v
                     val (if (instance? clojure.lang.IDeref val) @val val)]
                 (->> outs
                      (map
                       (fn [[k [_ opts]]]
                         (str (:id v) "->" (:id k) "[label=\"" (pr-str opts) "\"];\n")))
                      (cons
                       (format
                        "%d[label=\"%d (%s)\"];\n"
                        (:id v) (:id v) (pr-str val))))))))
          (apply str)
          (format "digraph g {
node[color=black,style=filled,fontname=Inconsolata,fontcolor=white,fontsize=9];
edge[fontname=Inconsolata,fontsize=9];
ranksep=1;
overlap=scale;
%s}")
          (spit path))))
