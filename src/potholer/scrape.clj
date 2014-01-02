(ns potholer.scrape
  (:require [clojure.data.csv :as csv]
            [clojure.java.io  :as io]
            [clojure.walk     :as walk]
            [clojure.set      :as set]
            [clojure.pprint   :as pp]
            [clojure.string   :as str]))

                                        ; TODO

;; * TODO: Better way to do pre/post conditions? Is there a better
;; abstraction for dealing with them?
;; * TODO: Handle empty string in default-values?
;; * TODO: set-value! => awkward.
;; * TODO: Compute should be able to delete keys as well.

                                        ; FLAGS [DEVELOPMENT]
(set! *print-length* 100)
(set! *print-level* 10)

                                        ; CSV
(defn lazy-csv [path]
  (let [in-file (io/reader path)
        csv-seq (csv/read-csv in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                (if-let [s (seq wrapped)]
                  (cons (first s) (lazy (rest s)))
                  (.close in-file))))]
    (lazy csv-seq)))

(defn with-headers [[headers & rows]]
  (map #(-> (zipmap headers %)
            (walk/keywordize-keys))
       rows))

                                        ; TRANSFORMATION FUNCTIONS

;; PRE/POST CONDITIONS
(defn- matching-keys? [m coll]
  (every? #(contains? m %) coll))

;; RENAME
(defn rename [m txmap]
  (set/rename-keys m txmap))

(defn rename->> [txmap ms]
  (map #(rename % txmap) ms))

;; TRIM
(defn- strict-trim [v]
  (if (string? v) (str/trim v) v))

(defn trim [m ks]
  (reduce (fn [memo k]
            (let [v (get m k)]
              (if v
                (assoc memo k (strict-trim v))
                m)))
          m
          ks))

(defn trim->> [ks ms]
  (map #(trim % ks) ms))

;; DEFAULT VALUES
(defn default-values [m txmap]
  (merge-with (fn [lval rval]
                (if (nil? lval)
                  rval
                  lval))
              m
              txmap))

(defn default-values->> [txmap ms]
  (map #(default-values % txmap) ms))

;; SET
(defn set-value! [_ new-value] new-value)

(defn set-all [m ks new-value]
  (reduce (fn [memo k]
            (assoc memo k new-value))
          m
          ks))

(defn set-all->> [ks new-value ms]
  (map #(set-all % ks new-value) ms))

;; CONVERT
(defn convert [m f ks]
  (reduce (fn [memo k] (assoc memo k (f (get m k))))
          m
          ks))

(defn convert->> [f ks ms]
  (map #(convert % f ks) ms))

;; COMPUTE
(defn compute
  [m to-field f]
  (assoc m to-field (f m)))

(defn compute->> [to-field f ms]
  (map #(compute % to-field f) ms))

;; DELETE
(defn delete [m ks-vec]
  (apply (partial dissoc m) ks-vec))

(defn delete->> [ks-vec ms]
  (map #(delete % ks-vec) ms))

                                        ; GLOSSARY
(defn pprint-glossary [glossary]
    (pp/print-table
     [:original :readable]
     (map #(let [[original readable] %]
             (assoc {}
               :original original
               :readable readable))
          (into (sorted-map) @glossary))))

                                        ; TEST
(comment
  (def records (-> "resources/test.csv" lazy-csv with-headers))
  (def glossary (-> "resources/glossary.edn" slurp read-string atom))

  (->> (take 50 records)
       (rename->> @glossary)
       (set-all->> [:a] "lastname")
       (set-all->> [:b] "middle")
       (set-all->> [:c] "   first   ")
       (delete->> [:a])
       (trim->> [:a :b :c])
       (compute->> :foobar (fn [{:keys [a b c]}]
                             (str a ", " c " " b)))
       (delete->> [:junk-field])
       (convert->> #(.toUpperCase %) [:foobar])
       (set-all->> [:q :foos :r] 42)
       (default-values->> {:q 9999 :r 0000 :s 1111}))
)
