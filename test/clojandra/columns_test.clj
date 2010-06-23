(ns clojandra.columns-test
  (:use clojure.test)
  (:require [clojandra.columns :as columns]))

(deftest java-hashmap-to-map
  (is (= {} (columns/to-map (java.util.HashMap.))))
  (let [flat-m (doto (java.util.HashMap.)
                           (.put :a 1)
                           (.put :b 2))]
    (is (= {:a 1, :b 2} (columns/to-map flat-m)))
    (let [nested-m (doto (.clone flat-m) (.put :c flat-m))]
      (is (= {:a 1,
              :b 2,
              :c {:a 1, :b 2}} (columns/to-map nested-m))))))

(deftest name-value-seq-to-map
  (is (= {:a 1, :b 2} (columns/to-map [[:a 1] [:b 2]])))
  (is (= {:a 1, :b 2} (columns/to-map '([:a 1] [:b 2])))))

(deftest cassandra-column-to-map-entry
  (is (= ["a" "1"] (columns/to-map-entry
                     (doto (org.apache.cassandra.thrift.Column.)
                       (.setName (columns/to-bytes "a"))
                       (.setValue (columns/to-bytes "1")))))))

(deftest cassandra-supercolumn-to-map-entry
  (is (= ["a" [["1" "FOO"]]] (columns/to-map-entry
                     (doto (org.apache.cassandra.thrift.SuperColumn. (columns/to-bytes "a")
                       [(doto (org.apache.cassandra.thrift.Column.)
                         (.setName (columns/to-bytes "1"))
                         (.setValue (columns/to-bytes "FOO")))]))))))

(deftest cassandra-column-or-supercolumn-to-map-entry
  (is (= ["a" "1"] (columns/to-map-entry
                     (doto (org.apache.cassandra.thrift.ColumnOrSuperColumn.)
                       (.setColumn
                         (doto (org.apache.cassandra.thrift.Column.)
                           (.setName (columns/to-bytes "a"))
                           (.setValue (columns/to-bytes "1")))))))))

