(ns util.string-test
  (:use util.string
        clojure.test))

(deftest test-bytes-to-string
  (are [x y] (= x y)
    "abcd" (bytes->str (byte-array (map #(byte %) [97 98 99 100])))
    "ABCD" (bytes->str (byte-array (map #(byte %) [65 66 67 68])))))

(deftest test-string-to-bytes
  (are [x y] (= x y)
    [97 98 99 100] (seq (str->bytes "abcd"))
    [65 66 67 68]  (seq (str->bytes "ABCD"))))



