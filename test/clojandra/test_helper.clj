(ns clojandra.test-helper
  (:use clojandra.core
        clojure.test)
  (:import (org.apache.thrift.transport TTransportException)))

(defmacro test-cassandra [test-name & body]
  `(deftest ~test-name
     (try
       (with-client ["localhost" 9160]
         ~@body)
       (catch TTransportException ~'e
         (println "\n\n ==========\n|\n|"
                  "Cassandra isn't running on port 9160, and it should be.\n|\n"
                  "==========\n")
           (throw ~'e)))))
