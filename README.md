# potholer

A Clojure library designed to do transformations on sequences of maps.

## Usage

```clojure
(ns mything.core
  (:require [potholer.scrape :refer :all]))

(->> (-> "test.csv" lazy-csv with-headers)
     (trim->> [:a :b :c])
     (compute->> :newfield (fn [{:keys [a b c]}]
                             (str b ", " a " " c)))
     (delete->> [:junkfield1 :junkfield2])
     (convert->> #(.toUpperCase %) [:newfield])
     (default-values->> {:x 10 :y 2 :z 42}))
```

## License

Copyright © 2013 Devin Walters

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
