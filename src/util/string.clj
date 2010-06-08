(ns util.string)

(def encoding "UTF-8")

(defn bytes->str [bytes]
  (String. bytes encoding))

(defn str->bytes [s]
  (.getBytes s encoding))
