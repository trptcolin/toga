(ns toga.core-test
  (:use toga.core
        toga.test-helper
        clojure.test))

(test-cassandra cassandra-is-running)

(use-fixtures :each
  (fn [f]
    (with-client ["localhost" 9160]
      (clear-keyspace "CassandraClojureTestKeyspace1")
      (in-keyspace "CassandraClojureTestKeyspace1"
        (f)))))

(deftest getting-record-with-client-and-keyspace
  (with-client ["localhost" 9160 "CassandraClojureTestKeyspace1"]
    (insert "People" "colin" "full_name" "Colin Jones")
    (let [colin (get-record "People" "colin")]
      (is (= "Colin Jones" (colin "full_name"))))))

(deftest getting-all-keyspaces
  (let [keyspaces (get-all-keyspaces)]
    (is (= "CassandraClojureTestKeyspace1"
          (some #{"CassandraClojureTestKeyspace1"} keyspaces)))
    (is (= "CassandraClojureTestKeyspace2"
          (some #{"CassandraClojureTestKeyspace2"} keyspaces)))))

(deftest describing-nonexistent-keyspace
  (is (= {} (describe-keyspace "imaginary-keyspace"))))

(deftest describing-existing-keyspace
  (let [keyspace (describe-keyspace "CassandraClojureTestKeyspace1")
        events (keyspace "Events")]
    (is (= "org.apache.cassandra.db.marshal.UTF8Type" (events "CompareWith")))
    (is (re-matches #"(?s).*CassandraClojureTestKeyspace1\.Events.*"
          (events "Desc")))
    (is (re-matches #"(?s).*Column Family Type: Standard.*"
          (events "Desc")))
    (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.UTF8Type.*"
          (events "Desc")))))


(deftest getting-empty-record-with-real-column-family
  (is (= {} (get-record "CassandraClojureTestKeyspace1" "Events" "bogus_key")))
  (is (= {} (get-record "Events" "bogus_key"))))

(deftest getting-empty-record-with-nonexistent-column-family
  (is (= {} (get-record "CassandraClojureTestKeyspace1" "fake_column_family" "bogus_key")))
  (is (= {} (get-record "fake_column_family" "bogus_key"))))

(deftest getting-existing-record
  (insert "People" "colin" "full_name" "Colin Jones")
  (insert "People" "colin" "location" "Chicagoland")

  (let [colin (get-record "People" "colin")]
    (is (= "Colin Jones" (colin "full_name")))
    (is (= "Chicagoland" (colin "location")))))

(deftest deleting-a-record
  (insert "People" "kathy" "full_name" "Kathy Jones")
  (is (= {"full_name" "Kathy Jones"} (get-record "People" "kathy")))

  (delete-record "People" "kathy")
  (is (= {} (get-record "People" "kathy"))))

(deftest inserting-a-record
  (insert "People" "oscar" {"full_name" "Oscar Jones", "location" "Chicagoland"})
  (let [oscar (get-record "People" "oscar")]
    (is (= {"full_name" "Oscar Jones",
            "location", "Chicagoland"} (get-record "People" "oscar")))))

; NOTE: this will only work as you'd expect using an order-preserving partitioner
;         (set up in the Cassandra storage-conf.xml)
(deftest get-slice-of-keys
  (insert "People" "colin" {"full_name" "Colin Jones"})
  (insert "People" "oscar" {"full_name" "Oscar Jones"})
  (insert "People" "molly" {"full_name" "Molly Jones"})

  (is (= {"molly" {"full_name" "Molly Jones"}
          "oscar" {"full_name" "Oscar Jones"}}
        (get-records "CassandraClojureTestKeyspace1" "People" "l" "z"))))

(deftest with-supercolumns

  (deftest get-existing-keyspaces
    (in-keyspace "CassandraClojureTestKeyspace2"
      (let [keyspace (describe-keyspace "CassandraClojureTestKeyspace2")
            events (keyspace "People")]
        (is (re-matches #"(?s).*CassandraClojureTestKeyspace2\.People.*"
              (events "Desc")))
        (is (re-matches #"(?s).*Column Family Type: Super.*"
              (events "Desc")))
        (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.BytesType.*"
              (events "Desc"))))))

  (deftest inserting-and-getting
    (in-keyspace "CassandraClojureTestKeyspace2"
      (let [molly { "address" {"city" "Mundelein"
                               "state" "IL"
                               "zip" "60060"}}]
        (insert "People" "molly" molly)

        (is (= molly (get-record "People" "molly"))))))

  (deftest multiple-supercolumns
    (in-keyspace "CassandraClojureTestKeyspace2"
      (let [colin {"mailing" {"city" "Libertyville"
                              "state" "IL"
                              "zip" "60048"}
                   "email" {"domain" "8thlight.com"
                            "user" "colin"}}]

        (insert "People" "colin" colin)
        (is (= colin (get-record "People" "colin")))))))
