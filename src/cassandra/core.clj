(ns cassandra.core
  (:use util.string)
  (:import
    (org.apache.cassandra.thrift
      ConsistencyLevel Cassandra$Client ColumnPath
      SlicePredicate SliceRange ColumnParent
      KeyRange SuperColumn ColumnOrSuperColumn Column)
    (org.apache.thrift.transport TSocket)
    (org.apache.thrift.protocol TBinaryProtocol)))

(def *consistency-level* ConsistencyLevel/ONE)

(def *client*)
(def *keyspace*)

(defmacro with-client [[host port] & body]
  `(let [socket# (TSocket. ~host ~port)
         protocol# (TBinaryProtocol. socket#)]
     (binding [*client* (Cassandra$Client. protocol#)]
       (with-open [transport# socket#]
         (.open transport#)
         ~@body))))

(defmacro in-keyspace [keyspace & body]
  `(binding [*keyspace* ~keyspace]
     ~@body))


(defn- get-current-microseconds []
  (/ (System/nanoTime) 1000))

(defn- column->map [col]
  {:name (bytes->str (.getName col)),
   :value (bytes->str (.getValue col))
   :timestamp (.getTimestamp col)})

(defn- supercolumn->map [col]
  {:name (bytes->str (.getName col))
   :value (map column->map (.getColumns col))})

; TODO: A bunch of dumb Java construction procedures here.
;       Should they be extracted?
(defn- make-slice-range []
  (doto (SliceRange.)
    (.setStart (byte-array 0))
    (.setFinish (byte-array 0))))

(defn- make-slice-predicate []
  (doto (SlicePredicate.)
    (.setSlice_range (make-slice-range))))

(defn- make-key-range
  ([] (KeyRange.))
  ([start end] (doto (make-key-range) (.setStart_key start) (.setEnd_key end))))

(defn- make-column-path
  ([family] (ColumnPath. family))
  ([family column]
   (doto (make-column-path family)
     (.setColumn (str->bytes column)))))

(defn make-column [name value]
  (Column. (str->bytes name) (str->bytes value) (get-current-microseconds)))

(defn map->columns [column]
  (map (fn [[k v]] (make-column k v)) column))

(defn make-supercolumn [name columns]
  (SuperColumn. (str->bytes name)
    (map->columns columns)))

(defn initialize-column [column-or-supercolumn k v]
  (if (map? v)
    (.setSuper_column column-or-supercolumn (make-supercolumn k v))
    (.setColumn column-or-supercolumn (make-column k v))))

(defn make-column-or-supercolumn
  ([[k v]]
   (doto (ColumnOrSuperColumn.)
     (initialize-column k v))))

(defn- get-range-slices [keyspace column-family]
  (.get_range_slices
    *client*
    keyspace
    (ColumnParent. column-family)
    (make-slice-predicate)
    (make-key-range "" "")
    *consistency-level*))

(defn- get-records-in-column-family [keyspace cf]
  (map
    (fn [row] {:column-family cf :key (.getKey row) :columns (.getColumns row)})
    (get-range-slices keyspace cf)))

(defn get-all-keyspaces
  "Gets a sequence of the names of all keyspaces on *client*"
  [] (seq (.describe_keyspaces *client*)))

; TODO: Is this memoization a bad idea?
;       Currently, changing keyspaces requires a cassandra restart to re-read the xml
;       This strategy will obviously be problematic in other cases...
(def memoized-get-all-keyspaces
  (memoize get-all-keyspaces))


; TODO: The instance? check here seems wrong, but does seem to work...
;       How can an object NOT pass the isa? test for a Java class
;         but pass the instance? test?  That's what happens here.
;       Also, this doesn't check for java.util.Map as a key (admittedly rare)
(defn- to-hash-map [h]
  (apply
    hash-map
    (interleave
      (keys h)
      (map
        #(if (instance? java.util.Map %) (to-hash-map %) %)
        (vals h)))))

(defn describe-keyspace
  "Given a keyspace name for *client*, gets a hash-map of keyspace names
  mapped to a hash-map of keyspace settings"
  [keyspace]
  (to-hash-map
    (if (some #{keyspace} (memoized-get-all-keyspaces))
      (.describe_keyspace *client* keyspace)
      nil)))

(defmulti insert
  "Inserts a record"
  (fn [& args] (type (last args))))

(defmethod insert clojure.lang.IPersistentMap
  ([family k value-map]
   (insert *keyspace* family k value-map))
  ([keyspace family k value-map]
     (.batch_insert *client*
       keyspace
       k
       {family (map #(make-column-or-supercolumn %) value-map)}
       *consistency-level*)))

(defmethod insert java.lang.Object
  ([family k col value]
   (insert *keyspace* family k col value))
  ([keyspace family k col value]
   (.insert *client*
     keyspace
     k
     (make-column-path family col)
     (str->bytes value)
     (get-current-microseconds)
     *consistency-level*)))

; TODO: cache keyspaces to eliminate database calls + above
(defn- column-family-exists?
  ([column-family] (column-family-exists? *keyspace* column-family))
  ([keyspace column-family]
   (some #{column-family}
     (keys (describe-keyspace keyspace)))))

; naive - ignores SuperColumn
(defn- get-columns
  ([column-family k] (get-columns *keyspace* column-family k))
  ([keyspace column-family k]
   (if (column-family-exists? keyspace column-family)
     (let [parent (ColumnParent. column-family)]
       (map
         #(let [supercolumn (.getSuper_column %)]
            (if supercolumn
              (supercolumn->map supercolumn)
              (column->map (.getColumn %))))
         (.get_slice *client*
           keyspace
           k
           parent
           (make-slice-predicate)
           *consistency-level*)))
     nil)))

(defn mapcolumns->map [cols]
  (reduce (fn [x y] (into x {(:name y) (:value y)})) {} cols))

(defn name-value-reducer [x y]
  (if (seq? (:value y))
    (into x {(:name y) (mapcolumns->map (:value y))})
    (into x {(:name y) (:value y)})))

; TODO: We discard timestamp information here
;       Is that something there's a genuine use case for?
;       Wanted to add it as metadata on the column value, but alas...
(defn get-record
  "Get a record as a map of column names to values"
  ([column-family key] (get-record *keyspace* column-family key))
  ([keyspace column-family k]
   (let [cols (get-columns keyspace column-family k)]
     (reduce name-value-reducer {} cols))))

(defn delete-record
  ([column-family k] (delete-record *keyspace* column-family k))
  ([keyspace column-family k]
   (.remove *client*
     keyspace
     k
     (make-column-path column-family)
     (get-current-microseconds)
     *consistency-level*)))

(defn clear-column-family
  "Use with extreme caution. It will blow away all data for a ColumnFamily"
  ([column-family] (clear-column-family *keyspace* column-family))
  ([keyspace column-family]
   (let [records-by-cf (get-records-in-column-family keyspace column-family)]
     (doall
       (map
         (fn [record]
           (delete-record keyspace (:column-family record) (:key record)))
         records-by-cf)))))


(defn clear-keyspace
  "Use with extreme caution. This will blow away all data for a Keyspace"
  [keyspace]
  (let [cfs (keys (describe-keyspace keyspace))]
    (doall
      (map
        (fn [cf]
          (clear-column-family keyspace cf))
        cfs))))

