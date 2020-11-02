(ns konserve.gc
  (:require [konserve.core :as k]
            [clojure.core.async]
            [superv.async :refer [go-try- <?- reduce<?-]])
  #?(:clj (:import [java.util Date])))

(defn sweep! [store whitelist ts]
  (let [hinted-get-time (fn [x] #?(:clj (.getTime ^Date x)  ; TODO: Assess if this is provides any performance benefit
                                   :cljs (.getTime x)))]
    (go-try-
     (<?- (reduce<?-
           (fn [deleted-files {:keys [key konserve.core/timestamp] :as meta}]
             (go-try-
              (if (or (contains? whitelist key) (<= (hinted-get-time ts) (hinted-get-time timestamp)))
                deleted-files
                (do
                  (<?- (k/dissoc store key))
                  (conj deleted-files key)))))
           #{}
           (<?- (k/keys store)))))))
