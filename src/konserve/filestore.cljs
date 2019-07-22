(ns konserve.filestore
  (:require
   [cljs.nodejs :as node]
   [konserve.core :as k]
   [hasch.core :refer [uuid]]
   [incognito.edn :refer [read-string-safe]]
   [konserve.serializers :as ser]
   [fress.api :as fress]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get-in -update-in -assoc-in -dissoc
                               PJSONAsyncKeyValueStore -jget-in -jassoc-in -jupdate-in
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               -serialize -deserialize]]
   [cljs.core.async :as async :refer (take! <! >! put! take! close! chan poll!)]
   [cljs.tools.reader.impl.inspect :as i])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

;; TODO serializer
;; TODO spec konserve.core
(defonce fs (node/require "fs"))

(defonce fs (node/require "buffer"))

(defonce stream (node/require "stream"))

(defn delete-store
  "Permanently deletes the folder of the store with all files."
  [folder]
  (if (.existsSync fs folder)
    (try
      (doseq [path [(str folder "/meta") (str folder "/data") folder]]
        (when (.existsSync fs path)
          (doseq [file (.readdirSync fs path)]
            (let [file-path (str path "/" file)]
              (when (.existsSync fs file-path)
                (if (.isDirectory (.statSync fs file-path))
                  (.rmdirSync fs file-path)
                  (.unlinkSync fs file-path)))))))
      (catch js/Object err
        (println err))
      (finally
        (.rmdirSync fs folder)))
    "folder not exists"))

(defn check-and-create-folder
  "Creates Folder with given path."
  [path]
  (let [test-file (str path "/" (random-uuid))]
    (try
      (when-not (fs.existsSync path)
        (.mkdirSync fs path))
      (.writeFileSync fs test-file "test-data")
      (.unlinkSync fs test-file)
      (catch js/Object err
        (println err)))))

(defn write-edn-key [serializer write-handlers folder {:keys [key] :as meta}]
  (let [key       (uuid (first key))
        temp-file (str folder "/meta/" key ".new")
        new-file  (str folder "/meta/" key)
        fd        (.openSync fs new-file "w+")
        _         (.closeSync fs fd)
        ws        (.createWriteStream fs temp-file)
        res-ch    (chan)
        buf       (fress/byte-stream)]
    (.on ws "close" #(try (.renameSync fs temp-file new-file)
                          (catch js/Object err
                            (put! res-ch (ex-info "Could not write edn key."
                                                  {:type      :write-edn-key-error
                                                   :key       key
                                                   :exception err}))
                            (close! res-ch))
                          (finally (close! res-ch))))
    (.on ws "error"
         (fn [err]
           (put! res-ch (ex-info "Could not write edn key."
                                 {:type      :write-edn-key-error
                                  :key       key
                                  :exception err}))
           (close! res-ch)))
    (-serialize serializer buf write-handlers (update meta :key first))
    (.write ws (js/Uint8Array. (.from js/Array @buf)))
    (.end ws)
    res-ch))

(defn write-edn [serializer write-handlers read-handlers folder key up-fn]
  (let [key       (uuid (first key))
        temp-file (str folder "/data/" key ".new")
        new-file  (str folder "/data/" key)
        res-ch    (chan)]
    (if (.existsSync fs new-file)
      (let [rs          (.createReadStream fs new-file)
            data-buffer (atom {:buffer nil})]
        (.on rs "data" (fn [chunk]
                         (swap! data-buffer update :buffer (fn [old] (if (nil? old)
                                                                       chunk
                                                                       (js/Buffer.concat #js [old chunk]))))))
        (.on rs "close" #(let [ws    (.createWriteStream fs temp-file)
                               old   (-deserialize serializer read-handlers (:buffer @data-buffer))
                               value (up-fn old)
                               buf   (fress/byte-stream)]
                           (.on ws "finish" (fn [_]
                                              (.renameSync fs temp-file new-file)
                                              (put! res-ch [old value])
                                              (close! res-ch)))
                           (.on ws "error"
                                (fn [err]
                                  (put! res-ch (ex-info "Could not write edn."
                                                        {:type      :write-edn-error
                                                         :key       key
                                                         :exception err}))
                                  (close! res-ch)))
                           (-serialize serializer buf write-handlers value)
                           (.write ws (js/Uint8Array. (.from js/Array @buf)))
                           (.close ws)))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not write edn."
                                                {:type      :write-edn-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (let [fd    (.openSync fs new-file "w+")
            _     (.closeSync fs fd)
            ws    (.createWriteStream fs temp-file)
            buf   (fress/byte-stream)
            value (up-fn nil)]
        (.on ws "close" #(try
                           (.renameSync fs temp-file new-file)
                           (put! res-ch [nil value])
                           (close! res-ch)
                           (catch js/Object err
                             (put! res-ch (ex-info "Could not write edn."
                                                   {:type      :write-edn-error
                                                    :key       key
                                                    :exception err}))
                             (close! res-ch))))
        (-serialize serializer buf write-handlers value)
        (.write ws (js/Uint8Array. (.from js/Array @buf)))
        (.end ws)))
    res-ch))

(defn write-binary [folder key input]
  (let [res-ch (chan)]
    (try
      (let [file-name (uuid key)
            temp-file (str folder "/data/" file-name ".new")
            new-file  (str folder "/data/" file-name)
            ws        (.createWriteStream fs temp-file)
            input     (if (js/Buffer.isBuffer input)
                        (let [stream (stream.PassThrough)
                              _      (.end stream input)]
                          stream)
                        (if (instance? stream input)
                          input
                          (throw (js/Error. "Invalid input type"))))]
        (.on ws "close" #(try
                           (.renameSync fs temp-file new-file)
                           (catch js/Object err
                             (put! res-ch (ex-info "Could not write binary."
                                                   {:type      :write-binary-error
                                                    :key       key
                                                    :exception err})))
                           (finally
                             (.unpipe input ws)
                             (put! res-ch true)
                             (close! res-ch))))
        (.on ws "error" (fn [err] (put! res-ch (ex-info "Could not write binary."
                                                        {:type      :write-binary-error
                                                         :key       key
                                                         :exception err}))
                          (close! res-ch)))
        (.pipe input ws))
      (catch js/Object err
        (put! res-ch (ex-info "Could not write binary."
                              {:type      :write-binary-error
                               :key       key
                               :exception err}))
        (close! res-ch)))
    res-ch))

(defn read-edn [serializer read-handlers path key]
  (let [file-name (str path "/data/" (uuid key))
        res-ch    (chan)]
    (if (.existsSync fs file-name)
      (let [rs          (.createReadStream fs file-name)
            data-buffer (atom {:buffer nil})]
        (.on rs "data" (fn [chunk]
                         (swap! data-buffer update :buffer (fn [old] (if (nil? old)
                                                                       chunk
                                                                       (js/Buffer.concat #js [old chunk]))))))
        (.on rs "close" #(let [data (-deserialize serializer read-handlers (:buffer @data-buffer))]
                          (put! res-ch data)
                          (close! res-ch)))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not read edn."
                                                {:type      :read-edn-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (close! res-ch))
    res-ch))

(defn read-edn-key [serializer read-handlers path key]
  (let [file-name (str path "/meta/" key)
        res-ch    (chan)]
    (if (.existsSync fs file-name)
      (let [rs          (.createReadStream fs file-name)
            data-buffer (atom {:buffer nil})]
        (.on rs "data" (fn [chunk]
                         (swap! data-buffer update :buffer (fn [old] (if (nil? old)
                                                                       chunk
                                                                       (js/Buffer.concat #js [old chunk]))))))
        (.on rs "close" #(let [data    (-deserialize serializer read-handlers (:buffer @data-buffer))]
                          (put! res-ch data)
                          (close! res-ch)))
        (.on rs "error" (fn [err]
                          (put! res-ch (ex-info "Could not read edn key."
                                                {:type      :read-edn-key-error
                                                 :key       key
                                                 :exception err}))
                          (close! res-ch))))
      (close! res-ch))
    res-ch))

(defn read-binary
  "return read stream"
  [folder key locked-cb]
  (let [res       (chan)
        file-id   (uuid key)
        file-name (str folder "/data/" file-id)]
    (if (.existsSync fs file-name)
      (let [size (str (aget (.statSync fs file-name) "size") " Bytes")
            rs   (.createReadStream fs file-name)]
        (.on rs "close" #(do (put! res true) (close! res)))
        (.on rs "error" (fn [err]
                          (put! res (ex-info "Could not read binary."
                                             {:type      :read-binary-error
                                              :key       key
                                              :exception err}))
                          (close! res)))
        (go (>! res (<! (locked-cb {:read-stream rs
                                    :file        file-name
                                    :size        size})))
            (close! res)))
      (close! res))
    res))

(defn list-keys [{:keys [folder serializer]} read-handlers]
  (let [filenames
        (for [filename (filter #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" %) (js->clj (.readdirSync fs (str folder "/meta"))))]
          filename)]
    (->> filenames
         (map #(read-edn-key serializer read-handlers folder %))
         async/merge
         (async/into #{}))))

(defn delete-entry [folder file-name res-ch]
  (let [key-file (str folder "/meta/" file-name)
        data-file (str folder "/data/" file-name)]
    (.unlink fs key-file
             (fn [err]
               (if err
                 (if (= (aget err "code") "ENOENT")
                   (do (put! res-ch "File not exist")
                       (close! res-ch))
                   (throw err))
                 (do (.unlink fs data-file
                              (fn [err]
                                (if err
                                  (throw err))))
                     (put! res-ch true)
                     (close! res-ch)))))))

(defrecord FileSystemNodejsStore
    [folder serializer read-handlers write-handlers locks config]
  PEDNAsyncKeyValueStore
  (-exists? [this key]
    (let [fn  (uuid key)
          f   (str folder "/data/" fn)
          res (chan)]
      (put! res (.existsSync fs f))
      (close! res)
      res))
  (-get-in [this key-vec]
    (read-edn serializer read-handlers folder (first key-vec)))
  (-update-in [this key-vec up-fn]
    (go (<! (write-edn-key serializer write-handlers folder {:key key-vec :format :edn}))
        (<! (write-edn serializer write-handlers read-handlers folder key-vec up-fn))))
  (-assoc-in [this key-vec val] (-update-in this key-vec (fn [_] val)))
  (-dissoc [this key] (let [fn (uuid key)
                            res-ch (chan)]
                        (delete-entry folder fn res-ch)
                        res-ch))
  PBinaryAsyncKeyValueStore
  (-bget [this key locked-cb] (read-binary folder key locked-cb))
  (-bassoc [this key input] (do (write-edn-key serializer write-handlers folder {:key [key] :format :binary})
                                (write-binary folder key input))))

(defn new-fs-store
  "Filestore contains a Key and a Data Folder"
  [path & {:keys [read-handlers write-handlers serializer config]
           :or   {read-handlers  (atom {})
                  write-handlers (atom {})
                  serializer     (ser/fressian-serializer)
                  config         {:fsync true}}}]
  (let [_ (check-and-create-folder path)
        _ (check-and-create-folder (str path "/meta"))
        _ (check-and-create-folder (str path "/data"))]
    (go (map->FileSystemNodejsStore {:folder         path
                                     :serializer     serializer
                                     :read-handlers  read-handlers
                                     :write-handlers write-handlers
                                     :locks          (atom {})
                                     :config         config}))))
