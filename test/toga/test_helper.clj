(ns toga.test-helper
  (:refer-clojure :exclude (get))
  (:use toga
        clojure.test)
  (:import (org.apache.thrift.transport TTransportException)))

(defmacro warn-on-unavailable [& body]
  `(try
     ~@body
     (catch TTransportException ~'e
         (println "\n\n ==========\n|\n|"
                  "Cassandra isn't running on port 9160, and it should be.\n|\n"
                  "==========\n")
           (throw ~'e))))

