(ns cassandra.core)

(def encoding "UTF-8")
(def clevel org.apache.cassandra.thrift.ConsistencyLevel/ONE)

(def *host* "localhost")
(def *port* 9160)
(def *keyspace* "Keyspace1")

(defmacro with-client [[client [host port]] & body]
  `(let [socket# (org.apache.thrift.transport.TSocket. ~host ~port)
         protocol# (org.apache.thrift.protocol.TBinaryProtocol. socket#)
         ~'client (org.apache.cassandra.thrift.Cassandra$Client. protocol#)]
     (with-open [transport# socket#]
       (.open transport#)
       ~@body)))

(defn bytes->str [bytes]
  (String. bytes encoding))

(defn str->bytes [s]
  (.getBytes s encoding))

(defn make-column-path [family column]
  (doto (org.apache.cassandra.thrift.ColumnPath. family)
    (.setColumn (str->bytes column))))

(defn get-generic [family k col]
  (with-client [client [*host* *port*]]
    (.get client *keyspace* k (make-column-path family col) clevel)))

; Usage:
; (with-client [client ["localhost" 9160]]
;   (get-column "Standard1" "ccc" "celery"))
;
(defn get-column [family k col]
  (let [col (.getColumn (get-generic family k col))]
    {:name (bytes->str (.getName col)),
     :value (bytes->str (.getValue col))
     :timestamp (.getTimestamp col)}))
