(ns cassandra.core-test
  (:use cassandra.core
        cassandra.test-helper
        clojure.test))

(test-cassandra cassandra-is-running)

(deftest with-client-and-keyspace
  (with-client ["localhost" 9160 "CassandraClojureTestKeyspace1"]
      (insert "People" "colin" "full_name" "Colin Jones")
      (let [colin (get-record "People" "colin")]
        (is (= "Colin Jones" (colin "full_name"))))))

(deftest core-functionality
  (with-client ["localhost" 9160]
  ; TODO: Right now there's a manual step to create these keyspaces
  ;       Should it be automatic? (maybe a prompt if they already exist?)
  ;       This would probably mean running a script that populates the storage-conf.xml
  ;       Should it pull the test keyspaces back out at the end of the test run?
  ;       Should the tests fire up the Cassanda server automatically?

  ; This should be happen before every test, but apparently use-fixtures
  ;   doesn't do what we need. Roll our own?
  (clear-keyspace "CassandraClojureTestKeyspace1")
  (clear-keyspace "CassandraClojureTestKeyspace2")

  (testing "get all keyspaces"
    (let [keyspaces (get-all-keyspaces)]
      (is (= "CassandraClojureTestKeyspace1"
             (some #{"CassandraClojureTestKeyspace1"} keyspaces)))
      (is (= "CassandraClojureTestKeyspace2"
             (some #{"CassandraClojureTestKeyspace2"} keyspaces)))))

  (testing "describing nonexistent keyspace"
    (is (= {} (describe-keyspace "imaginary-keyspace"))))

  (testing "describing existing keyspace"
    (let [keyspace (describe-keyspace "CassandraClojureTestKeyspace1")
          events (keyspace "Events")]
      (is (= "org.apache.cassandra.db.marshal.UTF8Type" (events "CompareWith")))
      (is (re-matches #"(?s).*CassandraClojureTestKeyspace1\.Events.*"
                      (events "Desc")))
      (is (re-matches #"(?s).*Column Family Type: Standard.*"
                      (events "Desc")))
      (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.UTF8Type.*"
                      (events "Desc")))))


  (testing "getting empty record with real column family"
    (is (= {} (get-record "CassandraClojureTestKeyspace1" "Events" "bogus_key")))
    (in-keyspace "CassandraClojureTestKeyspace1"
      (is (= {} (get-record "Events" "bogus_key")))))

  (testing "getting empty record with nonexistent column family"
    (is (= {} (get-record "CassandraClojureTestKeyspace1" "fake_column_family" "bogus_key")))
    (in-keyspace "CassandraClojureTestKeyspace1"
      (is (= {} (get-record "fake_column_family" "bogus_key")))))

  (testing "getting existing record"
    (in-keyspace "CassandraClojureTestKeyspace1"
      (insert "People" "colin" "full_name" "Colin Jones")
      (insert "People" "colin" "location" "Chicagoland")

      (let [colin (get-record "People" "colin")]
        (is (= "Colin Jones" (colin "full_name")))
        (is (= "Chicagoland" (colin "location"))))))

  (testing "deleting a record"
    (in-keyspace "CassandraClojureTestKeyspace1"
      (insert "People" "kathy" "full_name" "Kathy Jones")
      (is (= {"full_name" "Kathy Jones"} (get-record "People" "kathy")))

      (delete-record "People" "kathy")
      (is (= {} (get-record "People" "kathy")))))

  (testing "inserting a record"
    (in-keyspace "CassandraClojureTestKeyspace1"
      (insert "People" "oscar" {"full_name" "Oscar Jones", "location" "Chicagoland"})
      (let [oscar (get-record "People" "oscar")]
        (is (= {"full_name" "Oscar Jones",
                "location", "Chicagoland"} (get-record "People" "oscar"))))))

  (testing "with supercolumns"
    (in-keyspace "CassandraClojureTestKeyspace2"

      (testing "get existing keyspaces"
        (let [keyspace (describe-keyspace "CassandraClojureTestKeyspace2")
              events (keyspace "People")]
          (is (re-matches #"(?s).*CassandraClojureTestKeyspace2\.People.*"
                (events "Desc")))
          (is (re-matches #"(?s).*Column Family Type: Super.*"
                (events "Desc")))
          (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.BytesType.*"
                (events "Desc")))))

      (testing "inserting and getting"
        (let [molly { "address" {"city" "Mundelein"
                                 "state" "IL"
                                 "zip" "60060"}}]
          (insert "People" "molly" molly)

          (is (= molly (get-record "People" "molly")))))

;      (testing "get nonexistent record (testing before)"
;        (is (= {} (get-record "People" "molly"))))
      ))

)
  )
