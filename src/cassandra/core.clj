(ns cassandra.core
  (:import
    (org.apache.cassandra.thrift
         ConsistencyLevel Cassandra$Client ColumnPath
         SlicePredicate SliceRange ColumnParent)
    (org.apache.thrift.transport TSocket)
    (org.apache.thrift.protocol TBinaryProtocol)))

(def encoding "UTF-8")
(def clevel ConsistencyLevel/ONE)

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

; TODO: move translation methods to another ns
(defn bytes->str [bytes]
  (String. bytes encoding))

(defn str->bytes [s]
  (.getBytes s encoding))

(defn make-column-path [family column]
  (doto (ColumnPath. family)
    (.setColumn (str->bytes column))))

(defn get-generic [family k col]
  (.get *client* *keyspace* k (make-column-path family col) clevel))

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
    clevel))

(defn make-slice-range []
  (doto (SliceRange.)
    (.setStart (byte-array 0))
    (.setFinish (byte-array 0))))

(defn make-slice-predicate [slice-range]
  (doto (SlicePredicate.)
    (.setSlice_range slice-range)))

; naive - ignores SuperColumn
(defn get-row [column-family k]
  (let [parent (ColumnParent. column-family)]
    (map
      #(column->map (.getColumn %))
      (.get_slice *client*
                  *keyspace*
                  k
                  parent
                  (make-slice-predicate (make-slice-range))
                  clevel))))

