(ns thi.ng.fabric.utils
  (:require
   [thi.ng.fabric.core :as f]))

(defn sorted-vertex-values
  [vertices]
  (->> vertices
       (sort-by f/id)
       (map (juxt f/id deref))))

#?(:clj
   (defn vertices->dot
     ([path vertices flt]
      (vertices->dot
       path vertices flt
       #(format "%d[label=\"%d (%s)\"];\n" (f/id %) (f/id %) %2)))
     ([path vertices flt fmt]
      (->> vertices
           (filter flt)
           (sort-by f/id)
           (mapcat
            (fn [v]
              (if-let [outs @(.-outs ^thi.ng.fabric.core.Vertex v)]
                (let [val @v
                      val (pr-str (if (instance? clojure.lang.IDeref val) @val val))]
                  (->> outs
                       (map
                        (fn [[k [_ opts]]]
                          (str (f/id v) "->" (f/id k) "[label=\"" (pr-str opts) "\"];\n")))
                       (cons (fmt v val)))))))
           (apply str)
           (format "digraph g {
node[color=black,style=filled,fontname=Inconsolata,fontcolor=white,fontsize=9];
edge[fontname=Inconsolata,fontsize=9];
ranksep=1;
overlap=scale;
%s}")
           (spit path)))))
