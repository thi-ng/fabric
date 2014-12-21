(ns thi.ng.fabric.utils)

(defn trace-v
  [pre v score]
  (prn pre (:id @v) score (dissoc @v :out :collect :score-sig :score-coll)))

(defn dump
  [g]
  (->> (:vertices @g)
       (vals)
       (map deref)
       (sort-by :id)
       (map (juxt :id :state))))

#+clj
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
