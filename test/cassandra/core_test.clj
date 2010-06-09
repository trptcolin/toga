(ns cassandra.core-test
  (:use cassandra.core
        cassandra.test-helper
        clojure.test
        clojure.contrib.mock))

(test-cassandra core-functionality
  (clear-keyspace "CassandraClojureTestKeyspace1")
  (clear-keyspace "CassandraClojureTestKeyspace2")

  (testing "describing-existing-keyspaces"
    (let [keyspaces (seq (describe-keyspaces))]
      (is (= "system" (some #{"system"} keyspaces)))
      (is (= "Keyspace1" (some #{"Keyspace1"} keyspaces)))))

  (testing "getting-keyspaces"
    (let [keyspaces (get-all-keyspaces)]
      (is (= "CassandraClojureTestKeyspace1" (some #{"CassandraClojureTestKeyspace1"} keyspaces)))
      (is (= "CassandraClojureTestKeyspace2" (some #{"CassandraClojureTestKeyspace2"} keyspaces)))))

  (testing "describing-nonexistent-keyspace"
    (is (= nil (describe-keyspace "imaginary-keyspace"))))

  (testing "describing-existing-keyspace"
    (let [keyspace (describe-keyspace "Keyspace1")]
      (is (= ["Standard1" "Standard2" "StandardByUUID1" "Super1" "Super2"]
             (sort (keys keyspace))))))

  (testing "getting-empty-record-with-real-column-family"
    (is (= []
           (get-record "Standard1" "bogus_key"))))

  (testing "getting-empty-record-with-nonexistent-column-family"
    (is (= nil
           (get-record "imaginary_column_family" "bogus_key"))))

  (testing "getting-existing-record"
    (is (= [{:name "dog" :value "Molly"} {:name "dog2" :value "Oscar"}
            {:name "name" :value "colin"} {:name "wifey" :value "kathy"}]
           (no-timestamps (get-record "Standard1" "ccc")))))
)
