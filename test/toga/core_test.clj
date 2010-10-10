(ns toga.core-test
  (:require [toga.core :as toga])
  (:use [toga.test-helper]
        [lazytest.describe :only (describe given it with)]
        [lazytest.context.stateful :only (stateful-fn-context)]))

(def datastore-context
  (stateful-fn-context
    (fn []
			(let [client (toga/open-client {:host "localhost" :port 9160})]
	      (binding [toga.core/*client* client]
          (toga/clear-keyspace "CassandraClojureTestKeyspace1"))
        client))
    (fn [client] (.close (toga/get-transport client)))))

(describe "inserting and getting a simple record"
  (toga/with-client {:host "localhost" :port 9160 :keyspace "CassandraClojureTestKeyspace1"}
    (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "colin" "full_name" "Colin Jones")
    (let [colin (toga/get "CassandraClojureTestKeyspace1" "Subscribers" "colin")]
      (it "pulls the right record back out"
        (= "Colin Jones" (colin "full_name"))))))

(describe "getting all keyspaces"
  (with [datastore-context]
      (it (some #{"CassandraClojureTestKeyspace1"}
          (binding [toga.core/*client* @datastore-context] (toga/get-all-keyspaces))))))

(describe "describing a nonexistent keyspace"
  (with [datastore-context]
    (it (= {} (toga/describe-keyspace "imaginary-keyspace")))))

(describe "describing an existing keyspace"
  (with [datastore-context]
    (letfn [(subscribers []
              (binding [toga.core/*client* @datastore-context]
                ((toga/describe-keyspace "CassandraClojureTestKeyspace1") "Subscribers")))]
      (it (= "org.apache.cassandra.db.marshal.UTF8Type" (( subscribers ) "CompareWith")))
      (it (re-matches #"(?s).*CassandraClojureTestKeyspace1\.Subscribers.*"
            (( subscribers ) "Desc")))
      (it (re-matches #"(?s).*Column Family Type: Standard.*"
            (( subscribers ) "Desc")))
      (it (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.UTF8Type.*"
            (( subscribers ) "Desc"))))))

(describe "getting an empty record with a real column family"
  (with [datastore-context]
    (it (= {}
           (binding [toga.core/*client* @datastore-context]
                    (toga/get "CassandraClojureTestKeyspace1" "Events" "bogus_key"))))))

(describe "getting an empty record with a nonexistent column family"
  (with [datastore-context]
    (it (= {}
           (binding [toga.core/*client* @datastore-context]
                    (toga/get "CassandraClojureTestKeyspace1" "fake_column_family" "bogus_key"))))))

(describe "getting an existing record"
  (with [datastore-context]
    (letfn [(colin [attr]
              (binding [toga.core/*client* @datastore-context]
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "colin" "full_name" "Colin Jones")
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "colin" "location" "Chicagoland")
                ((toga/get "CassandraClojureTestKeyspace1" "Subscribers" "colin") attr)))]
           (it (= "Colin Jones" (colin "full_name")))
           (it (= "Chicagoland" (colin "location"))))))

(describe "deleting a record"
  (with [datastore-context]
    (letfn [(kathy []
              (binding [toga.core/*client* @datastore-context]
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "kathy" "full_name" "Kathy Jones")
                (toga/delete-record "CassandraClojureTestKeyspace1" "Subscribers" "kathy")
                (toga/get "CassandraClojureTestKeyspace1" "Subscribers" "kathy")))]
      (it (= {} (kathy))))))

(describe "inserting a record"
  (with [datastore-context]
    (letfn [(oscar []
              (binding [toga.core/*client* @datastore-context]
                (toga/insert "CassandraClojureTestKeyspace1"
                             "Subscribers"
                             "oscar"
                             {"full_name" "Oscar Jones"
                              "location"   "Chicagoland"})
                (toga/get "CassandraClojureTestKeyspace1" "Subscribers" "oscar")))]
      (it (= {"full_name" "Oscar Jones", "location" "Chicagoland"}
             (oscar))))))

; NOTE: this will only work as you'd expect using an order-preserving partitioner
;         (set up in the Cassandra conf/storage-conf.xml)
(describe "get a slice of keys"
  (with [datastore-context]
    (letfn [(get-records []
              (binding [toga.core/*client* @datastore-context]
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "colin" {"full_name" "Colin Jones"})
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "oscar" {"full_name" "Oscar Jones"})
                (toga/insert "CassandraClojureTestKeyspace1" "Subscribers" "molly" {"full_name" "Molly Jones"})
                (toga/get-records "CassandraClojureTestKeyspace1" "Subscribers" "l" "z")))]
      (it (= {"molly" {"full_name" "Molly Jones"}
              "oscar" {"full_name" "Oscar Jones"}}
             (get-records))))))

(describe "get existing keyspaces"
  (with [datastore-context]
    (letfn [(people [attr]
              (binding [toga.core/*client* @datastore-context]
                (let [keyspace (toga/describe-keyspace "CassandraClojureTestKeyspace1")]
                  ((keyspace "People") attr))))]
      (it (re-matches #"(?s).*CassandraClojureTestKeyspace1\.People.*"
            (people "Desc")))
      (it (re-matches #"(?s).*Column Family Type: Super.*"
            (people "Desc")))
      (it (re-matches #"(?s).*Columns Sorted By: org.apache.cassandra.db.marshal.BytesType.*"
            (people "Desc"))))))

(describe "inserting and getting"
  (with [datastore-context]
    (let [molly { "address" {"city" "Mundelein"
                          "state" "IL"
                          "zip" "60060"}}]
      (letfn [(get-molly []
                (binding [toga.core/*client* @datastore-context]
                  (toga/insert "CassandraClojureTestKeyspace1" "People" "molly" molly)
                  (toga/get "CassandraClojureTestKeyspace1", "People" "molly")))]
        (it (= molly (get-molly)))))))

(describe "multiple supercolumns"
  (with [datastore-context]
    (let [colin {"mailing" {"city" "Libertyville"
                            "state" "IL"
                            "zip" "60048"}
                 "email" {"domain" "8thlight.com"
                          "user" "colin"}}]
      (letfn [(get-colin []
                (binding [toga.core/*client* @datastore-context]
                  (toga/insert "CassandraClojureTestKeyspace1" "People" "colin" colin)
                  (toga/get "CassandraClojureTestKeyspace1" "People" "colin")))]
          (it (= colin (get-colin)))))))
