(ns exch.utils
  (:require
   [clojure.string :as str]
   [clojure.data.json :as json]
   [clojure.tools.logging :refer [warn]]
   [aleph.http :as http]
   [byte-streams :as bs]
   [manifold.stream :as ms]))

(import [java.time Instant LocalDateTime ZoneOffset])


(def intervals-map {:1m 60 :3m 180 :5m 300 :15m 900 :30m 1800
                    :1h 3600 :2h 7200 :4h 14400 :6h (* 6 3600) :8h (* 8 3600) :12h (* 12 3600)
                    :1d 86400 :3d (* 3 86400) :1w (* 7 86400) :1M (* 31 86400) :1mo (* 31 86400)})

(defprotocol Exchange
  "A protocol that abstracts exchange interactions"
  (open-streams     [this streams])
  (trade-stream     [this pairs fields])
  (depth-stream     [this pairs fields])
  (agg-trade-stream [this pairs fields])
  (candle-stream    [this kind tf pairs fields])
  (get-all-pairs    [this] "Return all pairs for current market")
  (gather-ws-loop!  [this push-raw! verbose] "Gather raw data via websockets")
  (get-candles      [this kind fields interval pair start end])
  (order-ticker     [this pair fields])
  (get-balance      [this acc-keys fields])
  (get-positions    [this acc-keys fields])
  (place-order!'    [this ks pair order-type side args])
  (setup!'          [this ks pair args])
  (get-rec          [this kind]))

(defn place-order!
  [exchange keys pair type side & args]
  (place-order!' exchange keys pair type side args))

(defn setup! [this ks pair & args] (setup!' this ks pair args))

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

(defn inc-ts
  [ts tf & {:keys [mul] :or {mul 1}}]
  (case tf
    :1M (-> (LocalDateTime/ofEpochSecond (long (/ ts 1000)) 0 ZoneOffset/UTC)
            (.plusMonths mul)
            (.toEpochSecond ZoneOffset/UTC)
            (* 1000))
    (+ ts (* mul 1000 (tf intervals-map)))))

(defn dec-ts
  [ts tf & {:keys [mul] :or {mul 1}}]
  (inc-ts ts tf :mul (- mul)))

(defn now-ts [] (System/currentTimeMillis))

(defn ts-to-instant [ts] (Instant/ofEpochMilli ts))

(defn ts-str
  [ts & args]
  (if (nil? ts)
    "<nil>"
    (apply str (java.sql.Timestamp. ts) args)))

(defn candle-seq
  [exchange kind tf pair & {:keys [start end limit fields] :or {end (now-ts) limit (:candles-limit exchange)}}]
  (let [ts (atom start)]
    (apply concat
           (for [_ (range)
                 :let [start @ts]
                 :while (< start end)]
             (do (swap! ts inc-ts tf :mul (dec limit))
                 (with-retry 5
                   (get-candles exchange kind fields tf pair start @ts)))))))

(defn encode-params
  [params]
  (->> params
       (partition 2)
       (filter (comp some? last))
       (map (fn [[k v]]
              (str (name k) "=" (str v))))
       (str/join "&")))

(defn take-json! [ws] (some->> ws ms/take! deref json/read-str))

(def stream-seq! (comp (partial remove nil?) repeatedly (partial partial take-json!)))

(defn parse-double' [s] (Double/parseDouble s))
(defn parse-float   [s] (Float/parseFloat s))
(defn parse-int     [s] (Long/parseLong s))
(defn sql-ts [t] (java.sql.Timestamp. t))
(defn getter [k] #(get % k))

(defn assert-get [m k]
  (let [r (m k)]
    (assert r (str "Unknown key " k))
    r))

(defn field-parser
  [keys rec]
  (apply juxt
         (map (comp
               (partial apply comp)
               (partial map #(if (or (string? %) (int? %)) (getter %) %))
               reverse
               (partial assert-get rec))
              keys)))

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
  (let [encoded (apply url-encode-params url params)]
    (try
      (-> encoded
          (verb {:headers headers})
          deref
          :body
          bs/to-string
          json/read-str)
      (catch Exception e
        (warn "http-request-json: Encoded URL was" encoded)
        (throw e)))))

(def http-get-json (partial http-request-json http/get))
(def http-post-json (partial http-request-json http/post))

(defn ws-client
  [url & params]
  (let [url (apply url-encode-params url params)]
    (try
      (-> url http/websocket-client deref)
      (catch Exception e
        (warn "ws-client: Encoded URL was" url)
        (throw e)))))
