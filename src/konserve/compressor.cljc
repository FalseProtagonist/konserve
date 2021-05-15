(ns konserve.compressor
  (:require [konserve.protocols :refer [PStoreSerializer -serialize -deserialize]]
            [konserve.utils :refer [invert-map]])
  #?(:clj 
     (:import [net.jpountz.lz4 LZ4FrameOutputStream LZ4FrameInputStream]))
  )

(defrecord NullCompressor [serializer]
  PStoreSerializer
  (-deserialize [_ read-handlers bytes]
    (-deserialize serializer read-handlers bytes))
  (-serialize [_ bytes write-handlers val]
    (-serialize serializer bytes write-handlers val)))

#?(:clj 
    (defrecord Lz4Compressor [serializer]
      PStoreSerializer
      (-deserialize [_ read-handlers bytes]
        (let [lz4-byte (LZ4FrameInputStream. bytes)]
          (-deserialize serializer read-handlers lz4-byte)))
      (-serialize [_ bytes write-handlers val]
        (let [lz4-byte (LZ4FrameOutputStream. bytes)]
          (-serialize serializer lz4-byte write-handlers val)
          (.flush lz4-byte)))))

#?(:clj
   (defn lz4-compressor [serializer]
      (Lz4Compressor. serializer)))

(defn null-compressor [serializer]
  (NullCompressor. serializer))


#?(:clj   
    (def byte->compressor
      {0 null-compressor
       1 lz4-compressor})
   :cljs 
    (def byte->compressor
      {0 null-compressor}))

(def compressor->byte
  (invert-map byte->compressor))

