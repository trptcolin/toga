(ns all-tests
  (:use clojure.test))

(def test-nses
  [
   'toga.core-test
   'toga.columns-test
   'util.string-test
   ])

(doall
  (map #(use %) test-nses))

(apply run-tests test-nses)

