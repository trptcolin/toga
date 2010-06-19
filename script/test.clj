(ns all-tests
  (:use clojure.test))

(def test-nses
  [
   'clojandra.core-test
   'util.string-test
   ])

(doall
  (map #(use %) test-nses))

(apply run-tests test-nses)

