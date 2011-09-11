(ns fleetdb.client
  (:require [clojure.data.json :as json])
  (:import (java.net Socket URI))
  (:import (java.io OutputStreamWriter BufferedWriter
                    InputStreamReader BufferedReader
                    Closeable))
  (:import (clojure.lang IFn ILookup)))

(defn- doquery [#^BufferedWriter writer #^BufferedReader reader q keywordize]
  (let [#^String req (json/json-str q)]
    (if-let [resp (do (.write writer req)
                      (.write writer "\r\n")
                      (.flush writer)
                      (.readLine reader))]
      (let [[status result] (json/read-json resp keywordize)]
        (if (zero? status)
          result
          (throw (Exception. #^String result))))
      (throw (Exception. "No response from server.")))))

(defn query
  ([client q keywordize]
     (doquery (:writer client) (:reader client) q keywordize))
  ([client q]
     (query client q false)))

(defn close [client]
  (.close #^BufferedReader (:reader client))
  (.close #^BufferedWriter (:writer client))
  (.close #^Socket         (:socket client)))

(defn- apply-url [url options]
  (let [url-parsed (URI. url)]
    (-> options
      (dissoc :url)
      (assoc :host (.getHost url-parsed))
      (assoc :port (.getPort url-parsed))
      (assoc :password (if-let [ui (.getUserInfo url-parsed)]
                         (second (re-find #":(.+)" ui)))))))

(defn connect [& [options]]
  (if-let [url (:url options)]
    (connect (apply-url url options))
    (let [host       (get options :host "127.0.0.1")
          port       (get options :port 3400)
          timeout    (get options :timeout)
          password   (get options :password)
	  keywordize (get options :keywordize)
          socket     (Socket. #^String host #^Integer port)
          writer     (BufferedWriter. (OutputStreamWriter. (.getOutputStream  socket)))
          reader     (BufferedReader. (InputStreamReader.  (.getInputStream   socket)))
          attrs      {:writer writer :reader reader :socket socket
                      :host host :port port :password password :timeout timeout}]
      (when timeout
        (.setSoTimeout socket (int (* timeout 1000))))
      (when password
        (doquery writer reader ["auth" password]))
      (proxy [IFn ILookup Closeable] []
        (invoke [q] (doquery writer reader q keywordize))
        (valAt  [k] (attrs k))
        (close  []  (close attrs))))))
