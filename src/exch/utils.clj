(ns exch.utils
  (:require
   [clojure.string :as str]
   [clojure.data.json :as json]
   [aleph.http :as http]
   [byte-streams :as bs]))

(import java.net.URLEncoder)

(defprotocol Exchange
  "A protocol that abstracts exchange interactions"
  (get-all-pairs [this] "Return all pairs for current market")
  (gather-ws-loop! [this push-raw! verbose] "Gather raw data via websockets")
  (get-candles [this kind pair interval start end]))

(defn now-ts [] (System/currentTimeMillis))

(defn ts-str
  [ts & args]
  (if (nil? ts)
    "<nil>"
    (apply str (java.sql.Timestamp. ts) args)))

(defn encode-params
  [params]
  (->> params
       (partition 2)
       (filter (comp some? last))
       (map (fn [[k v]]
              (str (name k) "=" (-> v str (URLEncoder/encode "UTF-8") (.replaceAll "\\+" "%20")))))
       (str/join "&")))

(defn url-encode-params
  "Encode params in url"
  [url & params]
  (if
   (empty? params)
    url
    (->> params encode-params (str url "?"))))

(defn http-request-json
  "Request JSON data with HTTP GET request"
  [verb url & {:keys [params headers]}]
  (-> (apply url-encode-params url params)
      (verb {:headers headers})
      deref
      :body
      bs/to-string
      json/read-str))

(def http-get-json (partial http-request-json http/get))
(def http-post-json (partial http-request-json http/post))

(defmacro with-retry
  "body must return non false value"
  [tries & body]
  (let [e (gensym 'e) left (gensym 'left) result (gensym 'result) wait (gensym 'wait)]
    `(let [~result (atom nil)]
       (loop [~left (dec ~tries)]
         (when
          (try
            (reset! ~result (do ~@body))
            false
            (catch clojure.lang.ExceptionInfo ~e
              (if-let [~wait (-> ~e ex-data :retry-after)]
                (do
                  (warn (str "Exception (tries left " ~left "): " (ex-message ~e)))
                  (if (pos? ~left)
                    (do
                      (Thread/sleep ~wait)
                      true)
                    (throw ~e)))
                (throw ~e))))
           (recur (dec ~left))))
       @~result)))

