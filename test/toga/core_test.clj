(ns toga.core-test
  (:require [toga])
  (:use toga.test-helper
        clojure.test))

(use-fixtures :each
  (fn [f]
    (warn-on-unavailable
      (toga/with-client {:host "localhost" :port 9160}
        (toga/clear-keyspace "CassandraClojureTestKeyspace1")
        (toga/in-keyspace "CassandraClojureTestKeyspace1"
          (f))))))

(deftest getting-record-with-client-and-keyspace
  (toga/with-client {:host "localhost" :port 9160 :keyspace "CassandraClojureTestKeyspace1"}
    (toga/insert "Subscribers" "colin" "full_name" "Colin Jones")
    (let [colin (toga/get "Subscribers" "colin")]
      (is (= "Colin Jones" (colin "full_name"))))))

(deftest getting-all-keyspaces
  (let [keyspaces (toga/get-all-keyspaces)]
    (is (= "CassandraClojureTestKeyspace1"
          (some #{"CassandraClojureTestKeyspace1"} keyspaces)))))

(deftest describing-nonexistent-keyspace
  (is (= {} (toga/describe-keyspace "imaginary-keyspace"))))

(deftest describing-existing-keyspace
  (let [keyspace (toga/describe-keyspace "CassandraClojureTestKeyspace1")
        subscribers (keyspace "Subscribers")]
    (is (= "org.apache.cassandra.db.marshal.UTF8Type" (subscribers "CompareWith")))
    (is (re-matches #"(?s).*CassandraClojureTestKeyspace1\.Subscribers.*"
          (subscribers "Desc")))
    (is (re-matches #"(?s).*Column Family Type: Standard.*"
          (subscribers "Desc")))
    (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.UTF8Type.*"
          (subscribers "Desc")))))


(deftest getting-empty-record-with-real-column-family
  (is (= {} (toga/get "CassandraClojureTestKeyspace1" "Events" "bogus_key")))
  (is (= {} (toga/get "Events" "bogus_key"))))

(deftest getting-empty-record-with-nonexistent-column-family
  (is (= {} (toga/get "CassandraClojureTestKeyspace1" "fake_column_family" "bogus_key")))
  (is (= {} (toga/get "fake_column_family" "bogus_key"))))

(deftest getting-existing-record
  (toga/insert "Subscribers" "colin" "full_name" "Colin Jones")
  (toga/insert "Subscribers" "colin" "location" "Chicagoland")

  (let [colin (toga/get "Subscribers" "colin")]
    (is (= "Colin Jones" (colin "full_name")))
    (is (= "Chicagoland" (colin "location")))))

(deftest deleting-a-record
  (toga/insert "Subscribers" "kathy" "full_name" "Kathy Jones")
  (is (= {"full_name" "Kathy Jones"} (toga/get "Subscribers" "kathy")))
  (toga/delete-record "Subscribers" "kathy")
  (is (= {} (toga/get "Subscribers" "kathy"))))

(deftest inserting-a-record
  (toga/insert "Subscribers" "oscar" {"full_name" "Oscar Jones", "location" "Chicagoland"})
  (let [oscar (toga/get "Subscribers" "oscar")]
    (is (= {"full_name" "Oscar Jones",
            "location", "Chicagoland"} (toga/get "Subscribers" "oscar")))))

; NOTE: this will only work as you'd expect using an order-preserving partitioner
;         (set up in the Cassandra conf/storage-conf.xml)
(deftest get-slice-of-keys
  (toga/insert "Subscribers" "colin" {"full_name" "Colin Jones"})
  (toga/insert "Subscribers" "oscar" {"full_name" "Oscar Jones"})
  (toga/insert "Subscribers" "molly" {"full_name" "Molly Jones"})

  (is (= {"molly" {"full_name" "Molly Jones"}
          "oscar" {"full_name" "Oscar Jones"}}
        (toga/get-records "CassandraClojureTestKeyspace1" "Subscribers" "l" "z"))))

(deftest with-supercolumns

  (deftest get-existing-keyspaces
    (let [keyspace (toga/describe-keyspace "CassandraClojureTestKeyspace1")
          people (keyspace "People")]
      (is (re-matches #"(?s).*CassandraClojureTestKeyspace1\.People.*"
            (people "Desc")))
      (is (re-matches #"(?s).*Column Family Type: Super.*"
            (people "Desc")))
      (is (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.BytesType.*"
            (people "Desc")))))

  (deftest inserting-and-getting
    (let [molly { "address" {"city" "Mundelein"
                             "state" "IL"
                             "zip" "60060"}}]
      (toga/insert "People" "molly" molly)
      (is (= molly (toga/get "People" "molly")))))

  (deftest multiple-supercolumns
    (let [colin {"mailing" {"city" "Libertyville"
                            "state" "IL"
                            "zip" "60048"}
                 "email" {"domain" "8thlight.com"
                          "user" "colin"}}]
      (toga/insert "People" "colin" colin)
      (is (= colin (toga/get "People" "colin"))))))
