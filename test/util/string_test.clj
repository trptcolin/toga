(ns util.string-test
  (:use [util.string :as str]
				[lazytest.describe :only (describe it)]))

(describe "bytes -> string"
	(it (= "abcd" (str/bytes->str (byte-array (map #(byte %) [97 98 99 100])))))
	(it (= "ABCD" (str/bytes->str (byte-array (map #(byte %) [65 66 67 68]))))))

(describe "string -> bytes"
  (it (= [97 98 99 100] (seq (str/str->bytes "abcd"))))
	(it (= [65 66 67 68]  (seq (str/str->bytes "ABCD")))))

