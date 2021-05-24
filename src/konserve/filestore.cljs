(ns konserve.filestore
  (:require
   ["fs" :as fs]
   ["rimraf" :as rimraf]
   [konserve.serializers :refer [byte->key byte->serializer serializer-class->byte key->serializer fressian-serializer]]
   [konserve.encryptor :refer [null-encryptor encryptor->byte]]
   [konserve.compressor :refer [byte->compressor, null-compressor compressor->byte]]
   [hasch.core :refer [uuid]]
   [superv.async :refer [go-try- <?-]]
   [konserve.protocols :refer [PEDNAsyncKeyValueStore -exists? -get -get-meta -update-in -assoc-in -dissoc
                               PBinaryAsyncKeyValueStore -bget -bassoc
                               PStoreSerializer -serialize -deserialize]]
   [cljs.core.async :as async :refer [take! <! >! put! take! close! chan poll!]])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(def version 1)

(def header-size 8)

(defn get-file-name [data-key folder] 
  (str folder "/" (uuid data-key) ".ksv"))

(defn translate-node-stream-channel
  "fold-like utility function"
  [^js/ReadStream stream data-fn result-fn error-fn]
  (let [data-buffer (atom {:buffer nil})
        res-ch      (chan)]
    (.on stream "close"
         #(do
            (result-fn res-ch (:buffer @data-buffer))))
    (.on  stream "error"
          (fn [err]
            (put! res-ch (error-fn err)) (close! res-ch)))
    ;; provided last as this one makes it live
    (.on stream "data"
         #(swap! data-buffer update :buffer (data-fn %1)))
    res-ch))

(defn simple-chunk-concat [byte-chunk]
  (fn [old]
    (if (nil? old)
      byte-chunk
      (js/Buffer.concat #js [old byte-chunk]))))

(defn- combined-error [er error-msg msg]
  (ex-info error-msg (assoc msg :exception er)))

(defn key-error [e msg] (combined-error e "Could not read key." msg))

(defn unpack-file-buffer
  "equivalent of completion-read-handler, only thing they actually share is error handling"
  [msg exec-fn]
  (fn [res-ch byte-buffer]
    (try
      (exec-fn res-ch byte-buffer)
      (catch js/Object e
        #(do
           (put! res-ch (key-error e msg)))))))

(defn exec-unpack-header-buffer [res-ch ^js/Buffer header-buffer]
  (let [[version-byte serializer-byte compressor-byte encryptor-byte]
        (into [] (.slice header-buffer 0 4))
        meta-size (.readInt32BE header-buffer 4)]
      ;; these are called ids in header-handler 
      ;; but from the code they seem to be bytes not ids
    (put! res-ch [version-byte serializer-byte compressor-byte meta-size])
    res-ch))

(defn get-serialization-data [path msg]
  (let [rs (.createReadStream fs path #js {"options" #js {"end" (- header-size 1)}})
        ch
        (translate-node-stream-channel
         rs
         simple-chunk-concat
         (unpack-file-buffer msg exec-unpack-header-buffer)
         #(key-error %1 msg))]
      ch))

(defn exec-read-fn [read-fn]
  (fn [res-ch byte-buffer]
    (put! res-ch (read-fn byte-buffer))))

(defn get-file-size [file-name]
  (. (.statSync fs file-name) -size))

(defn compose-read-fn [serializer read-handlers {:keys [compressor encryptor]}]
  (let [apply-fn (partial -deserialize (compressor (encryptor serializer)) read-handlers)]
    apply-fn))

(defn read-edn-data [path msg start stop read-fn]
  (let
   [rs   (.createReadStream
          fs
          path
          #js {"start" start "end" stop})]
    (translate-node-stream-channel
     rs
     simple-chunk-concat
     (unpack-file-buffer msg (exec-read-fn read-fn))
     #(key-error %1 msg))))

(defn get-edn-meta-data [file-name read-handlers env]
  (go (let [[_ _ _ meta-size]
            (<! (get-serialization-data file-name {}))]
        (<! (read-edn-data
             file-name
             {}
             header-size
             (+ meta-size header-size)
             (compose-read-fn (fressian-serializer) read-handlers env))))))

(defn get-edn-data [file-name read-handlers env]
  (go (let [[_ _ _ meta-size]
            (<! (get-serialization-data file-name {}))]
        (<! (read-edn-data
             file-name
             {}
             (+ meta-size header-size)
             (get-file-size file-name)
             (compose-read-fn (fressian-serializer) read-handlers env))))))

(defn write-bytes [bytes ^js ws error-fn]
  (let [res-ch (chan)]
    (println "writing bytes")
    ;; TODO commented this, does that leave writestream open?
    ;; (.on ws "finish" #(put! res-ch true))
    (.on ws "error" (fn [err]
                      (put! res-ch (error-fn err))
                      (close! res-ch)))
    (.write ws bytes)
    (put! res-ch true)
    ;; (.close ws)
    res-ch))

(defn inst-to-str-key [inst]
  (pr-str (type inst)))

(defn simple-er [er] er)

(defn to-byte-array
  [version serializer compressor encryptor meta-size]
  (let [buf (.alloc js/Buffer 8)
        serializer-byte (serializer-class->byte (inst-to-str-key serializer))
        compressor-byte (compressor->byte compressor)]
    (.writeUInt8 buf version 0)
    (.writeUInt8 buf serializer-byte 1)
    (.writeUInt8 buf compressor-byte 2)
    (.writeUInt8 buf (encryptor->byte encryptor) 3)
    (.writeUInt32BE buf meta-size 4)
    buf))

(defn write-header [version serializer compressor encryptor meta-size ws]
  (let [byte-array
        (to-byte-array
         version
         serializer
         compressor
         encryptor
         meta-size)]
    (write-bytes
     byte-array
     ws
     simple-er)))

(defn write-edn
  [serialize-fn ws ^js value]
  ;; I think there's no equivalent of java AsyncFileChannel's write method
    ;; taking a start and stop byte it just writes everything
  (println "in write-edn")
  ;; temporal-avet, avet, eavt failed, rest passed
  (if (:temporal-avet-key value)
    (do (println "got temporal-avet-key")
        (println "type " (type (:temporal-avet-key value)))
        (try (:storage-addr (:temporal-avet-key value))
             (catch js/Object e (println "couldn't get storage-addr" e)))
        (try (println 
              "type storage-addr " 
              (type (:storage-addr (:temporal-avet-key value))))
             (catch js/Object e (println "couldn't get type storage-addr" e)))

        ;; all failed
        (try (serialize-fn (:temporal-avet-key value))
             (catch js/Object e (println "failed to serialize temporal-avet " e)))
        ;; (try (serialize-fn (:temporal-eavt-key value))
        ;;      (catch js/Object e (println "failed to serialized temporal-eavt-key")))
        ;; (try (serialize-fn (:avet-key value))
        ;;      (catch js/Object e (println "failed to serialize avet-key")))
        ;; (try (serialize-fn (:eavt-key value))
        ;;      (catch js/Object e (println "failed to serialize eavt-key")))
        ;; (try (serialize-fn (:temporal-aevt-key value))
        ;;      (catch js/Object e (println "failed to serialize temporal-aevt-key")))
        ;; (try (serialize-fn (:aevt-key value))
        ;;      (catch js/Object e (println "failed to serialized aevt")))

      ;; all passed
        ;; (try (serialize-fn (:config value))
        ;;      (catch js/Object e (println "failed to serialize config")))
        ;; (try (serialize-fn (:schema value))
        ;;      (catch js/Object e (println "failed to serialize schema")))
        ;; (try (serialize-fn (:rschema value))
        ;;      (catch js/Object e (println "failed to serialized rschema")))
        )
    (do (println "didn't get temporal-avet-key")))
  (println "got past checking for temporal-avet-key")
  (let [serialized-value (serialize-fn value)]
         (println "serialized the value")
         (write-bytes
          serialized-value
          ws
          simple-er))
       )

(defn compose-write-fn
  ;; -1 because cljs implementation doesn't use byte output stream
  ;; creates their own buffer
  [serializer write-handlers {:keys [compressor encryptor]}]
  (fn [val] (-serialize (encryptor (compressor serializer)) -1 write-handlers val)))

(defn callback-to-chan
  "utility function to call a node callback function that can only return an error
    and push the (error|true) onto a channel"
  [f & args]
  (let [c (chan)
        cb (fn [e] 
             (put! c (or e true)))]
    (apply f (concat args [cb]))
    c))

(defn update-file
;; pieced from update-file in filestore.clj and write-edn(-key) in scrap.cljs
  "Write file into filesystem. It write first the meta-size, that is stored in (1Byte),
  the meta-data and the actual data."
  [folder path serializer write-handlers buffer-size
   [key & rkey]
   {:keys [compressor encryptor version file-name up-fn up-fn-args
           up-fn-meta config operation input]}
   [old-meta old-value]]
  (let [path-new
        (str file-name ".new")
      ;; want to write and create
         ;; createWriteStream should be fine
        meta                 (up-fn-meta old-meta)
        value                (when (= operation :write-edn)
                               (if-not (empty? rkey)
                                 (apply update-in old-value rkey up-fn up-fn-args)
                                 (apply up-fn old-value up-fn-args)))
        write-fn             (compose-write-fn serializer write-handlers {:compressor compressor :encryptor encryptor})
        meta-bytes           (write-fn meta)
        meta-bytes-size      (.-byteLength meta-bytes)
        start-byte           (+ header-size meta-bytes-size)
      ;; move fsync config into the file open
      ;; https://github.com/nodejs/node/issues/28513
        ws                   (if (:fsync config)
                               (.createWriteStream fs path-new #js {"flags" "as"})
                               (.createWriteStream fs path-new))]
    (go
      (println "writing header")
      (<! 
       (write-header version serializer compressor encryptor meta-bytes-size ws))
      (println "writing meta")
      (<! 
       (write-edn  write-fn ws meta))
      (<! 
       (if (= operation :write-binary)
            (do
              (println "write-binary called uh oh")
              (throw ":write-binary updating file not implemented"))
            (do
              (println "writing data with value " value)
              (println "extra print")
              (println "inner " 
                       (<!
                        (write-edn write-fn ws value)))
              (println "wrote data! " value)
              )))
      (.close ws)
    ;; seems to indicate here that rename is atomic
    ;; https://gist.github.com/coolaj86/992478/1b859936fec8b85454c5d56f9332c973a478e71b
      (println "renaming path-new" path-new "file-name " file-name)
      (go
        (<! 
         (callback-to-chan #(.rename fs %1 %2 %3) path-new file-name)))
      (if (= operation :write-edn) [old-value value] true))))

(defn io-operation
  [key-vec folder serializers read-handlers write-handlers buffer-size
   {:keys [detect-old-files operation default-serializer] :as env}]
  (println "io-operation called, op " operation)
  (go
    (let [key           (first key-vec)
          msg           {:key key :operation operation}
          file-name     (get-file-name key folder)
          env           (assoc env :file-name file-name)
          serializer    (get serializers default-serializer)
          serialization-data-res
          (<! (get-serialization-data file-name msg))
                   ;; otherwise will be error
          file-exists? (vector? serialization-data-res)]
      (if file-exists?
        (case operation
          :read-version
          (nth (<! (get-serialization-data file-name msg)) 0)
          :read-serializer
          (nth (<! (get-serialization-data file-name msg)) 1)
          :read-meta
          (<! (get-edn-meta-data file-name read-handlers env))
          :read-edn
          (<! (get-edn-data file-name read-handlers env))
          :read-binary
          (throw (ex-info "read-binary not implemented" {}))
          :write-binary
          (throw (ex-info "write-binary not implemented" {}))
          :write-edn
          (let [old-meta (<! (get-edn-meta-data file-name read-handlers env))
                old-edn (<! (get-edn-data file-name read-handlers env))]
            (<! (update-file folder file-name serializer write-handlers
                             buffer-size key-vec env [old-meta old-edn]))))
        (case operation
          :write-edn
          (<! (update-file folder file-name serializer write-handlers
                           buffer-size key-vec env [nil nil]))
          :write-binary
          (throw (ex-info "write-binary not implemented" {}))
          nil)))))

(defn check-folder-writeable [folder]
  ;; not using uuid file as in java version because that creates 
  ;; possibility of stochastic error
  (go (let [
            test-file-path (str folder "/" "test-file")
            write-res (<! (callback-to-chan #(.writeFile fs test-file-path "test-string" #js{}%1)))
            delete-res (<! (callback-to-chan #(.unlink fs %1 %2) test-file-path))]
        (not (instance? js/Error write-res)))))


(defn check-and-create-folder [folder]
  ;; not using uuid file as in java version because that creates 
  ;; possibility of stochastic error
  (go (let [mkdir-res (<! (callback-to-chan #(.mkdir fs %1 %2) folder))]
        (<! (check-folder-writeable folder)))))

(defn delete-file 
  "Remove/Delete key-value pair of Filestore by given key. If success it will return true."
  [key folder]
    (let [path (get-file-name key folder)]
      (callback-to-chan #(.unlink fs %1 %2) path)))

(defrecord FileSystemStore [folder serializers default-serializer compressor
                            encryptor read-handlers write-handlers buffer-size
                            detect-old-version locks config]
  PEDNAsyncKeyValueStore
  (-get [this key]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation :read-edn
                   :compressor compressor
                   :encryptor encryptor
                   :format    :data
                   :version version
                   :default-serializer default-serializer
                   :detect-old-files detect-old-version
                   :msg       {:type :read-edn-error
                               :key  key}}))
  (-get-meta [this key]
    (io-operation [key] folder serializers read-handlers write-handlers buffer-size
                  {:operation :read-meta
                   :compressor compressor
                   :encryptor encryptor
                   :detect-old-files detect-old-version
                   :default-serializer default-serializer
                   :version version
                   :msg       {:type :read-meta-error
                               :key  key}}))
  (-assoc-in [this key-vec meta-up val]
    (-update-in this key-vec meta-up (fn [_] val) []))
  (-update-in [this key-vec meta-up up-fn args]
    (io-operation key-vec folder serializers read-handlers write-handlers buffer-size
                  {:operation  :write-edn
                   :compressor compressor
                   :encryptor encryptor
                   :detect-old-files detect-old-version
                   :version version
                   :default-serializer default-serializer
                   :up-fn      up-fn
                   :up-fn-args args
                   :up-fn-meta meta-up
                   :config     config
                   :msg        {:type :write-edn-error
                                :key  (first key-vec)}}))
  (-dissoc [this key]
    (delete-file key folder)))

(defn new-fs-store
  [path & {:keys [default-serializer serializers compressor encryptor
                  read-handlers write-handlers buffer-size config detect-old-file-schema?]
           :or   {default-serializer :FressianSerializer
                  compressor         null-compressor
                  encryptor          null-encryptor
                  read-handlers      (atom {})
                  write-handlers     (atom {})
                  buffer-size        (* 1024 1024)
                  config             {:fsync true}}}]
  (let [_       (check-and-create-folder path)
        store  (map->FileSystemStore {:detect-old-version false
                                      :folder             path
                                      :default-serializer default-serializer
                                      :serializers        (merge key->serializer serializers)
                                      :compressor         compressor
                                      :encryptor          encryptor
                                      :read-handlers      read-handlers
                                      :write-handlers     write-handlers
                                      :buffer-size        buffer-size
                                      :locks              (atom {})
                                      :config             config})]
    (go store)))

(defn delete-store 
  "Permanently deletes the folder of the store with all files."
  [folder]
  (callback-to-chan #(rimraf folder %1)))