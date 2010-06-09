(ns cassandra.core
  (:use util.string)
  (:import
    (org.apache.cassandra.thrift
         ConsistencyLevel Cassandra$Client ColumnPath
         SlicePredicate SliceRange ColumnParent
         KeyRange)
    (org.apache.thrift.transport TSocket)
    (org.apache.thrift.protocol TBinaryProtocol)))

(def consistency-level ConsistencyLevel/ONE)

(def *client*)
(def *keyspace*)

(defmacro with-client [[host port keyspace] & body]
  `(let [socket# (TSocket. ~host ~port)
         protocol# (TBinaryProtocol. socket#)]
     (binding [*keyspace* ~keyspace
               *client* (Cassandra$Client. protocol#)]
       (with-open [transport# socket#]
         (.open transport#)
         ~@body))))

(defn get-current-microseconds []
  (long (* (System/currentTimeMillis) 1000)))

(defn column->map [col]
    {:name (bytes->str (.getName col)),
     :value (bytes->str (.getValue col))
     :timestamp (.getTimestamp col)})

(defn make-slice-range []
  (doto (SliceRange.)
    (.setStart (byte-array 0))
    (.setFinish (byte-array 0))))

(defn make-slice-predicate []
  (doto (SlicePredicate.)
    (.setSlice_range (make-slice-range))))

(defn make-key-range
  ([] (KeyRange.))
  ([start end] (doto (make-key-range) (.setStart_key start) (.setEnd_key end))))


(defn get-range-slices [keyspace column-family]
  (.get_range_slices
    *client*
    keyspace
    (ColumnParent. column-family)
    (make-slice-predicate)
    (make-key-range "" "")
    consistency-level))

(defn get-records-in-column-family [keyspace cf]
  (map
    (fn [row] {:column-family cf :key (.getKey row) :columns (.getColumns row)})
    (get-range-slices keyspace cf)))

(defn get-columns-in-record [record]
  (doall
    (map
      (fn [column]
        (column->map (.getColumn column)))
      (:columns record))))

(defn describe-keyspaces []
  (.describe_keyspaces *client*))

(defn get-all-keyspaces []
  (vec (describe-keyspaces)))

; TODO: cache get-all-keyspaces lookup
(defn describe-keyspace [keyspace]
  (if (some #{keyspace} (get-all-keyspaces))
    (.describe_keyspace *client* keyspace)
    nil))

(defn make-column-path
  ([family] (ColumnPath. family))
  ([family column]
    (doto (make-column-path family)
      (.setColumn (str->bytes column)))))

(defn get-generic [family k col]
  (.get *client* *keyspace* k (make-column-path family col) consistency-level))

; Usage:
; (with-client [client ["localhost" 9160 "Keyspace1"]]
;   (get-column "Standard1" "ccc" "celery"))
;
(defn get-column [family k col]
  (let [col (.getColumn (get-generic family k col))]
    (column->map col)))

(defn insert [family k col value]
  (.insert *client*
    *keyspace*
    k
    (make-column-path family col)
    (str->bytes value)
    (get-current-microseconds)
    consistency-level))

; naive - ignores SuperColumn
; TODO: cache keyspaces to eliminate network calls + above

(defn column-family-exists? [column-family]
  (some #{column-family}
        (keys (describe-keyspace *keyspace*))))

(defn get-record [column-family k]
    (if (column-family-exists? column-family)
      (let [parent (ColumnParent. column-family)]
        (map
          #(column->map (.getColumn %))
          (.get_slice *client*
            *keyspace*
            k
            parent
            (make-slice-predicate)
            consistency-level)))
      nil))

(defn no-timestamps [record]
  (map #(dissoc % :timestamp) record))

(defn delete-record [column-family k col]
  (.remove *client*
           *keyspace*
           k
           (if col (make-column-path column-family col) (make-column-path column-family))
           (get-current-microseconds)
           consistency-level))

(defn clear-column-family [keyspace column-family]
  (let [records-by-cf (get-records-in-column-family keyspace column-family)]
    (doall
      (map
        (fn [record]
          (binding [*keyspace* keyspace]
            (delete-record (:column-family record) (:key record) (:column record))))
        records-by-cf))))

(defn clear-keyspace [keyspace]
  (let [cfs (keys (describe-keyspace keyspace))]
    (doall
      (map
        (fn [cf]
          (clear-column-family keyspace cf))
        cfs))))


