(ns toga.columns-test
  (:use [lazytest.describe :only (describe given it)])
  (:require [toga.columns :as columns]))

(describe "Converting a Java HashMap to a Clojure Map"
  (it "converts an empty map"
    (= {} (columns/to-map (java.util.HashMap.))))
  (given [flat-m (doto (java.util.HashMap.)
                       (.put :a 1)
                       (.put :b 2))]
      (it "converts a one-level map"
        (= {:a 1, :b 2} (columns/to-map flat-m)))
      (it "converts a nested map"
        (let [nested-m (doto (.clone flat-m) (.put :c flat-m))]
          (it (= {:a 1,
                  :b 2,
                  :c {:a 1, :b 2}} (columns/to-map nested-m)))))))

(describe "Converting a sequence of pairs to a map"
  (it "converts a vector of pairs"
    (= {:a 1, :b 2} (columns/to-map [[:a 1] [:b 2]])))
  (it "converts a list of pairs"
    (= {:a 1, :b 2} (columns/to-map '([:a 1] [:b 2])))))

(describe "Converting Cassandra objects to pairs (map entries)"
  (it "converts a column"
    (= ["a" "1"] (columns/to-map-entry
                   (doto (org.apache.cassandra.thrift.Column.)
                         (.setName (columns/to-bytes "a"))
                         (.setValue (columns/to-bytes "1"))))))
  (it "converts a supercolumn"
    (= ["a" [["1" "FOO"]]]
       (columns/to-map-entry
         (doto (org.apache.cassandra.thrift.SuperColumn. (columns/to-bytes "a")
           [(doto (org.apache.cassandra.thrift.Column.)
             (.setName (columns/to-bytes "1"))
             (.setValue (columns/to-bytes "FOO")))])))))

  (it "converts a ColumnOrSuperColumn"
    (= ["a" "1"]
       (columns/to-map-entry
         (doto (org.apache.cassandra.thrift.ColumnOrSuperColumn.)
           (.setColumn
             (doto (org.apache.cassandra.thrift.Column.)
               (.setName (columns/to-bytes "a"))
               (.setValue (columns/to-bytes "1")))))))))

