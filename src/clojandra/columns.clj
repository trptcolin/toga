(ns clojandra.columns
  (:use util.string)
  (:import
    (org.apache.cassandra.thrift
      SuperColumn Column ColumnOrSuperColumn Mutation Deletion)))

(defn timestamp [] (/ (System/nanoTime) 1000))

(defmulti to-bytes type)

(defmethod to-bytes String [x]
  (str->bytes x))


(defmulti to-map type)

(defmethod to-map java.util.Map [m]
  (into {} (map
             (fn [[k v]]
               (if (instance? java.util.Map v)
                 [k (to-map v)]
                 [k v]))
             m)))

(defmethod to-map clojure.lang.ISeq [cols]
  (reduce (fn [x y] (into x {(:name y) (:value y)})) {} cols))

(defmethod to-map Column [col]
  {:name (bytes->str (.getName col)),
   :value (bytes->str (.getValue col))
   :timestamp (.getTimestamp col)})

(defmethod to-map SuperColumn [col]
  {:name (bytes->str (.getName col))
   :value (map to-map (.getColumns col))})

(defmethod to-map ColumnOrSuperColumn [x]
  (to-map
    (if (.isSetColumn x)
      (.getColumn x)
      (.getSuper_column x))))

(defmethod to-map :default [x] {})


(defn make-column [name value]
  (Column. (to-bytes name) (to-bytes value) (timestamp)))

(defn map->columns [column]
  (map (fn [[k v]] (make-column k v)) column))

(defn make-supercolumn [name columns]
  (SuperColumn. (to-bytes name)
    (map->columns columns)))

(defn initialize-column [column-or-supercolumn k v]
  (if (map? v)
    (.setSuper_column column-or-supercolumn (make-supercolumn k v))
    (.setColumn column-or-supercolumn (make-column k v))))

(defn make-column-or-supercolumn
  ([[k v]]
   (doto (ColumnOrSuperColumn.)
     (initialize-column k v))))

(defmulti make-mutation type)

(defmethod make-mutation Deletion
  [deletion]
  (throw (Exception. "Deletion isn't implemented yet.")))

(defmethod make-mutation :default
  [x]
  (doto (Mutation.)
    (.setColumn_or_supercolumn (make-column-or-supercolumn x))))

