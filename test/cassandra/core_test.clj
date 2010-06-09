(ns cassandra.core-test
  (:use cassandra.core
        cassandra.test-helper
        clojure.test
        clojure.contrib.mock)
  (:import (org.apache.cassandra.thrift ColumnParent)))

(test-cassandra describing-existing-keyspaces
  (let [keyspaces (seq (describe-keyspaces))]
    (is (= "system" (some #{"system"} keyspaces)))
    (is (= "Keyspace1" (some #{"Keyspace1"} keyspaces)))))

(test-cassandra getting-keyspaces
  (is (= ["JournalColonel" "Keyspace1" "system"]
         (sort (get-all-keyspaces)))))

(test-cassandra describing-nonexistent-keyspace
  (is (= nil (describe-keyspace "imaginary-keyspace"))))

(test-cassandra describing-existing-keyspace
  (let [keyspace (describe-keyspace "Keyspace1")]
    (is (= ["Standard1" "Standard2" "StandardByUUID1" "Super1" "Super2"]
           (sort (keys keyspace))))))

(test-cassandra getting-empty-record-with-real-column-family
  (is (= []
         (get-record "Standard1" "bogus_key"))))

(test-cassandra getting-empty-record-with-nonexistent-column-family
  (is (= nil
         (get-record "imaginary_column_family" "bogus_key"))))

(test-cassandra getting-existing-record
  (is (= [{:name "dog" :value "Molly"} {:name "dog2" :value "Oscar"}
          {:name "name" :value "colin"} {:name "wifey" :value "kathy"}]
         (no-timestamps (get-record "Standard1" "ccc")))))
