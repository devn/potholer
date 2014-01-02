(ns potholer.scrape
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.walk :as walk]
            [clojure.set :as set]
            [clojure.string :as str]))

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
              (assoc memo k (strict-trim v))))
          m
          ks))

(defn trim->> [ks ms]
  (map #(trim % ks) ms))

;; DEFAULT VALUES
(defn default-values [m txmap]
  (merge-with (fn [l-val r-val]
                (if (nil? l-val)
                  r-val
                  l-val))
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
  ;; {:pre [(matching-keys? m ks)]}
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
  (map #(apply (partial dissoc %) ks-vec) ms))

                                        ; TEST
(comment
  (def records (-> "test.csv" lazy-csv with-headers))
  (def glossary (-> "resources/glossary.edn" slurp read-string atom))

  (->> (take 50 records)
       (rename->> @glossary)
       (trim->> [:a :b :c])
       (compute->> :foobar (fn [{:keys [a b c]}]
                             (str a ", " c b)))
       (delete->> :junk-field)
       (convert->> #(seq %) [:foobar])
       (convert->> #(set-value! % nil) [:error_3]) ;; TODO: 
       (default-values->> {:q 9999 :r 0000 :s 1111})

       (set-all->> [:q :foobar :r] 42))

  (require '[clojure.pprint :as pp])
  (defn pprint-glossary []
    (pp/print-table
     [:original :readable]
     (map #(let [[original readable] %]
             (assoc {}
               :original original
               :readable readable))
          (into (sorted-map) @glossary))))
)
