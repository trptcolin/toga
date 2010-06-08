(ns all-tests
  (:use clojure.test))

(def test-nses
  [
   'cassandra.core-test
   'util.string-test
   ])

(doall
  (map #(use %) test-nses))

(apply run-tests test-nses)

