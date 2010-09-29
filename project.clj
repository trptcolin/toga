(defproject toga "0.0.1"
  :description "Cassandra Client for Clojure"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [log4j/log4j "1.2.14"]
                 [org.slf4j/slf4j-api "1.5.8"]
                 [org.slf4j/slf4j-log4j12 "1.5.8"]
                 [org.clojars.trptcolin/apache-cassandra "0.6.2"]
                 [org.clojars.trptcolin/libthrift "r917130"]]
  :dev-dependencies [[com.stuartsierra/lazytest "1.0.0"]]
  :repositories {"stuartsierra.com" "http://stuartsierra.com/maven2"})

