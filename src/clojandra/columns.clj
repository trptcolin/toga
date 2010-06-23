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
(defmulti to-map-entry type)

(defmethod to-map java.util.Map [m] (into {} (map to-map-entry m)))
(defmethod to-map clojure.lang.Sequential [cols] (into {} cols))

(defmethod to-map :default [x] {})

(defmethod to-map-entry java.util.Map$Entry [[k v]]
  (if (instance? java.util.Map v)
    [k (to-map v)]
    [k v]))

(defmethod to-map-entry clojure.lang.MapEntry [[k v]]
  (if (seq? v)
    [k (to-map v)]
    [k v]))

(defmethod to-map-entry Column [col]
  (first {(bytes->str (.getName col)) (bytes->str (.getValue col))}))

(defmethod to-map-entry SuperColumn [col]
  (first {(bytes->str (.getName col))
          (map to-map-entry (.getColumns col))}))

(defmethod to-map-entry ColumnOrSuperColumn [x]
  (to-map-entry
    (if (.isSetColumn x)
      (.getColumn x)
      (.getSuper_column x))))

(defmethod to-map-entry :default [x] nil)


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

