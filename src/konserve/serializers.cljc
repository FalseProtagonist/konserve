(ns konserve.serializers
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
                      [incognito.fressian :refer [incognito-read-handlers
                                                  incognito-write-handlers]]
            #?@(:clj [[clojure.data.fressian :as fress]])
            #?@(:cljs [[fress.api :as fress]
                       [clojure.string :as str]])
            [incognito.edn :refer [read-string-safe]])
  #?(:clj (:import [java.io FileOutputStream FileInputStream DataInputStream DataOutputStream]
                   [org.fressian.handlers WriteHandler ReadHandler])))

#?(:clj
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (fress/read bytes
                   :handlers (-> (merge fress/clojure-read-handlers
                                        custom-read-handlers
                                        (incognito-read-handlers read-handlers))
                                 fress/associative-lookup)))

     (-serialize [_ bytes write-handlers val]
       (let [w (fress/create-writer bytes :handlers (-> (merge
                                                         fress/clojure-write-handlers
                                                         custom-write-handlers
                                                         (incognito-write-handlers write-handlers))
                                                        fress/associative-lookup
                                                        fress/inheritance-lookup))]
         (fress/write-object w val)))))

#?(:cljs
   (defrecord FressianSerializer [custom-read-handlers custom-write-handlers]
     PStoreSerializer
     (-deserialize [_ read-handlers bytes]
       (let [buf->arr (.from js/Array (.from js/Int8Array bytes))
             buf      (fress.impl.buffer/BytesOutputStream. buf->arr (count buf->arr))
             reader   (fress/create-reader buf
                                           :handlers (merge custom-read-handlers
                                                            (incognito-read-handlers read-handlers)))
             read     (fress/read-object reader)]
         read))
     (-serialize [_ bytes write-handlers val]
       (let [buf   (fress/byte-stream)
             writer (fress/create-writer buf 
                                         :handlers (merge custom-write-handlers
                                                          (incognito-write-handlers write-handlers)))]
         (fress/write-object writer val)
         (js/Uint8Array. (.from js/Array @buf))))))

(defn fressian-serializer
  ([] (fressian-serializer {} {}))
  ([read-handlers write-handlers] (map->FressianSerializer {:custom-read-handlers read-handlers
                                                            :custom-write-handlers write-handlers})))
(defrecord StringSerializer []
  PStoreSerializer
  (-deserialize [_ read-handlers s]
    (read-string-safe @read-handlers s))
  (-serialize [_ output-stream _ val]
    #?(:clj
       (binding [clojure.core/*out* output-stream]
         (pr val)))
    #?(:cljs
       (pr-str val))))

(defn string-serializer []
  (map->StringSerializer {}))

#?(:clj
   (defn construct->class [m]
     (->> (map (fn [[k v]] [(class v) k]) m)
          (into {}))))

#?(:cljs
   (defn construct->class [m]
     (->> (map (fn [[k v]] [(pr-str (type v)) k]) m)
          (into {}))))

(def byte->serializer
  {0 (string-serializer)
   1 (fressian-serializer)})

(def serializer-class->byte
  (construct->class byte->serializer))

#?(:clj
   (defn construct->keys [m]
     (->> (map (fn [[k v]] [(-> v class .getSimpleName keyword) v]) m)
          (into {}))))

;; commented because of issue below
;; #?(:cljs (defn to-kw [inst] (-> inst type pr-str (str/split "/") (nth 1) keyword)))

;; #?(:cljs
;;    (defn construct->keys [m]
;;      (->> (map (fn [[k v]] [(to-kw v) v]) m)
;;           (into {}))))

;; commented because of issue below
;; #_(:cljs
  ;;  (defn construct->keys [m] (map #(into [] [(to-kw %1) %1]) (vals m))))

#?(:clj
   (def key->serializer
     (construct->keys byte->serializer))
    ;; hardcoding this because the names of classes change
    ;; between shadow-cljs compile and shadow-cljs
    ;; maybe the root map byte->serializer should be refactored to 3-tuples (kw, byte, serializer)
    ;; but for now I'm just hardcoding it downstream here
  :cljs
   (def key->serializer 
     {:StringSerializer (string-serializer)
      :FressianSerializer (fressian-serializer)}))


(defn construct->byte [m n]
  (->> (map (fn [[k0 v0] [k1 v1]] [k0 k1]) m n)
       (into {})))

(def byte->key
  (construct->byte byte->serializer key->serializer))
