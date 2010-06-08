(ns cassandra.core-test
  (:use cassandra.core
        clojure.test
        clojure.contrib.mock)
  (:import (org.apache.cassandra.thrift ColumnParent)))

(deftest test-get-empty-record
  (def .get_slice)
  (binding [*client* :client
            *keyspace* :keyspace
            get-slice (fn [& args] [])]
    (expect [get-slice (has-args [:client
                                  :keyspace
                                  "colin"
                                  (ColumnParent. "family_members")
                                  (make-slice-predicate)
                                  consistency-level]
                          (times 1
                            (returns [])))]
      (is (= []
            (get-record "family_members" "colin"))))))

