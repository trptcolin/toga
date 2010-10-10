(ns toga.core
  (:refer-clojure :exclude (get))
  (:use toga.columns)
  (:import
    (org.apache.cassandra.thrift
      ConsistencyLevel Cassandra$Client
      ColumnParent ColumnPath
      SlicePredicate SliceRange KeyRange)
    (org.apache.thrift.transport TSocket)
    (org.apache.thrift.protocol TBinaryProtocol)))

(def *consistency-level* ConsistencyLevel/ONE)

(def *client*)

(defn make-socket [options]
  (let [{:keys [host port]} options]
    (TSocket. host port)))

(defn make-client [socket]
  (let [protocol (TBinaryProtocol. socket)]
    (Cassandra$Client. protocol)))

(defn open-client [options]
	(let [host 			(:host options)
				port 			(:port options)
			  socket 		(TSocket. host port)
				protocol	(TBinaryProtocol. socket)
				client 		(Cassandra$Client. protocol)]
		(.open socket)
		client))

(defn get-transport [client]
	(-> client
			(.getInputProtocol)
			(.getTransport)))

(defmacro with-client [options & body]
	`(let [client# (open-client ~options)]
		 (binding [*client* 	client#]
			 (with-open [transport# (get-transport client#)]
				 ~@body))))

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
     (.setColumn (to-bytes column)))))


(defn- get-range-slices [keyspace column-family start end]
  (.get_range_slices
    *client*
    keyspace
    (ColumnParent. column-family)
    (make-slice-predicate)
    (make-key-range start end)
    *consistency-level*))

(defn get-records [keyspace cf start end]
  (into {}
    (map
      (fn [row] [(.getKey row) (into {} (map to-map-entry (.getColumns row)))])
      (get-range-slices keyspace cf start end))))

(defn get-all-keyspaces
  "Gets a sequence of the names of all keyspaces on *client*"
  [] (seq (.describe_keyspaces *client*)))

; Currently, changing keyspaces requires a cassandra restart to re-read the xml
;   So memoizing rarely hurts in this case
(def memoized-get-all-keyspaces
  (memoize get-all-keyspaces))

(defn describe-keyspace
  "Given a keyspace name for *client*, gets a hash-map of keyspace names
  mapped to a hash-map of keyspace settings"
  [keyspace]
  (to-map
    (if (some #{keyspace} (memoized-get-all-keyspaces))
      (.describe_keyspace *client* keyspace)
      nil)))

(defmulti insert
  "Inserts a record"
  (fn [& args] (type (last args))))

(defmethod insert clojure.lang.IPersistentMap
  ([keyspace family k value-map]
     (.batch_mutate *client*
       keyspace
       {k {family (map #(make-mutation %) value-map)}}
       *consistency-level*)))

(defmethod insert :default
  ([keyspace family k col value]
   (.insert *client*
     keyspace
     k
     (make-column-path family col)
     (to-bytes value)
     (timestamp)
     *consistency-level*)))

; TODO: cache keyspaces to eliminate database calls?
(defn- column-family-exists?
 ([keyspace column-family]
   (some #{column-family}
     (keys (describe-keyspace keyspace)))))

(defn- get-columns
 ([keyspace column-family k]
   (if (column-family-exists? keyspace column-family)
     (map
       to-map-entry
       (.get_slice *client*
         keyspace
         k
         (ColumnParent. column-family)
         (make-slice-predicate)
         *consistency-level*))
     nil)))

; TODO: We discard timestamp information here
;       Is that something there's a genuine use case for?
;       Wanted to add it as metadata on the column value, but alas,
  ;       strings aren't proper Clojure objects with metadata
(defn get
  "Get a record as a map of column names to values"
	([keyspace column-family k]
     (reduce
       (fn [x y] (conj x (to-map-entry y)))
       {}
       (get-columns keyspace column-family k))))

(defn delete-record
	([keyspace column-family k]
   (.remove *client*
     keyspace
     k
     (make-column-path column-family)
     (timestamp)
     *consistency-level*)))

(defn clear-column-family
  "Use with extreme caution. It will blow away all data for a ColumnFamily"
	([keyspace column-family]
   (doall
     (map
       (fn [record] (delete-record keyspace column-family (.getKey record)))
       (get-range-slices keyspace column-family "" "")))))

(defn clear-keyspace
  "Use with extreme caution. This will blow away all data for an entire Keyspace"
  [keyspace]
  (doall
    (map
      (fn [cf] (clear-column-family keyspace cf))
      (keys (describe-keyspace keyspace)))))

