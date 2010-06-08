(ns cassandra.core
  (:use util.string)
  (:import
    (org.apache.cassandra.thrift
         ConsistencyLevel Cassandra$Client ColumnPath
         SlicePredicate SliceRange ColumnParent)
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

(defn make-column-path
  ([family] (ColumnPath. family))
  ([family column]
    (doto (make-column-path family)
      (.setColumn (str->bytes column)))))

(defn get-generic [family k col]
  (.get *client* *keyspace* k (make-column-path family col) consistency-level))

(defn column->map [col]
    {:name (bytes->str (.getName col)),
     :value (bytes->str (.getValue col))
     :timestamp (.getTimestamp col)})

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
    (System/currentTimeMillis)
    consistency-level))

(defn make-slice-range []
  (doto (SliceRange.)
    (.setStart (byte-array 0))
    (.setFinish (byte-array 0))))

(defn make-slice-predicate []
  (doto (SlicePredicate.)
    (.setSlice_range (make-slice-range))))

(defn get-slice [& args]
  `(.get_slice ~@args))

; naive - ignores SuperColumn
(defn get-record [column-family k]
  (let [parent (ColumnParent. column-family)]
    (map
      #(column->map (.getColumn %))
      (get-slice *client*
                  *keyspace*
                  k
                  parent
                  (make-slice-predicate)
                  consistency-level))))

(defn delete-record [column-family k col]
  (.remove *client*
           *keyspace*
           k
           (make-column-path column-family col)
           (System/currentTimeMillis)
           consistency-level))

