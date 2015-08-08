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
     ([path vertices flt]
      (vertices->dot
       path vertices flt
       #(format "%d[label=\"%d (%s)\"];\n" (:id %) (:id %) %2)))
     ([path vertices flt fmt]
      (->> vertices
           (filter flt)
           (sort-by :id)
           (mapcat
            (fn [v]
              (if-let [outs @(:outs v)]
                (let [val @v
                      val (pr-str (if (instance? clojure.lang.IDeref val) @val val))]
                  (->> outs
                       (map
                        (fn [[k [_ opts]]]
                          (str (:id v) "->" (:id k) "[label=\"" (pr-str opts) "\"];\n")))
                       (cons (fmt v val)))))))
           (apply str)
           (format "digraph g {
node[color=black,style=filled,fontname=Inconsolata,fontcolor=white,fontsize=9];
edge[fontname=Inconsolata,fontsize=9];
ranksep=1;
overlap=scale;
%s}")
           (spit path)))))
