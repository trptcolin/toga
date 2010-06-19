; TODO: sort out cassandra, thrift, log4j, sl4j dependencies
;
(defproject clojandra "0.0.1"
  :description "Cassandra Client for Clojure"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"] ]
  :repositories {"clojure-releases" "http://build.clojure.org/releases"}
  :main clojandra.core)


