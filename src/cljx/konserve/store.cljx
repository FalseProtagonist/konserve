(ns konserve.store
  "Address globally aggregated immutable key-value store(s)."
  (:require [konserve.protocols :refer [IEDNAsyncKeyValueStore]]
            #+clj [clojure.core.async :refer [go]])
  #+cljs (:require-macros [cljs.core.async.macros :refer [go]]))

(defrecord MemAsyncKeyValueStore [state tag-table]
  IEDNAsyncKeyValueStore
  (-get-in [this key-vec] (go (get-in @state key-vec)))
  (-assoc-in [this key-vec value] (go (swap! state assoc-in key-vec value)
                                      nil))
  (-update-in [this key-vec up-fn] (go [(get-in @state key-vec) ;; HACK, can be inconsistent! (but only old can be too old)
                                        (get-in (swap! state update-in key-vec up-fn) key-vec)])))

(defn new-mem-store
  ([] (new-mem-store (atom {})))
  ([init-atom] (new-mem-store init-atom (atom {})))
  ([init-atom tag-table]
     (go (MemAsyncKeyValueStore. init-atom tag-table))))
