(ns exch.binance
  (:require
   [clojure.string :as str]
   [clojure.walk :as w]
   [clojure.data.json :as json]
   [clojure.tools.logging :refer [debug info warn error]]
   [manifold.stream :as s]
   [aleph.http :as http]
   [exch.utils :as u]))

(import javax.crypto.Mac)
(import javax.crypto.spec.SecretKeySpec)
(import org.apache.commons.codec.binary.Hex)

(def binance-intervals
  "Chart intervals: (m)inutes, (h)ours, (d)ays, (w)eeks, (M)onths"
  ["1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d" "3d" "1w" "1M"])

(def binance-candles-limit 500)

(defn de-hyphen
  "Remove hyphens from string"
  [item]
  (clojure.string/replace item "-" ""))

(def lower-pair (comp clojure.string/lower-case de-hyphen)) ; Convert pair to lower name
(def upper-pair (comp clojure.string/upper-case de-hyphen)) ; Convert pair to upper name

(def stream-types {:t "@trade" :d "@depth"})

(def spot-url "https://www.binance.com/api")
(def spot-ws-url "wss://stream.binance.com:9443")
(def usdm-url "https://fapi.binance.com/fapi")
(def usdm-ws-url "wss://fstream.binance.com")

(def agg-trade-rec {:event-type   ["e"]
                    :event-ts     ["E"]
                    :symbol       ["s"]
                    :agg-trade-id ["a"]
                    :price        ["p" u/parse-double']
                    :quantity     ["q" u/parse-float]
                    :first-id     ["f"]
                    :last-id      ["l"]
                    :trade-ts     ["T"]
                    :trade-sql-ts ["T" u/sql-ts]
                    :trade-str-ts ["T" (comp str u/sql-ts)]
                    :buy?         ["m"]})

(def candle-ws-rec {:event-type   ["e"]
                :event-ts     ["E"]
                :symbol       ["s"]
                :open-ts      ["k" "t"]
                :open-sql-ts  ["k" "t" u/sql-ts]
                :open-str-ts  ["k" "t" (comp str u/sql-ts)]
                :close-ts     ["k" "T"]
                :close-sql-ts ["k" "T" u/sql-ts]
                :close-str-ts ["k" "T" (comp str u/sql-ts)]
                :inteval      ["k" "i"]
                :first-id     ["k" "f"]
                :last-id      ["k" "L"]
                :open         ["k" "o" u/parse-double']
                :close        ["k" "c" u/parse-double']
                :high         ["k" "h" u/parse-double']
                :low          ["k" "l" u/parse-double']
                :volume       ["k" "v" u/parse-float]
                :trades       ["k" "n"]
                :closed?      ["k" "x"]
                :quote        ["k" "q" u/parse-float] ; Quote asset volume
                :buy-volume   ["k" "V" u/parse-float]
                :buy-quote    ["k" "Q" u/parse-float] ; Taker buy quote asset volume
                })

(def candle-rec {:open-ts     [0]                 ; Open time
                 :open-sql-ts [0 u/sql-ts]
                 :open-str-ts [0 u/sql-ts str]
                 :open        [1 u/parse-double'] ; Open
                 :high        [2 u/parse-double'] ; High
                 :low         [3 u/parse-double'] ; Low
                 :close       [4 u/parse-double'] ; Close
                 :volume      [5 u/parse-float]   ; Volume
                 :close-ts    [6]                 ; Close time
                 :quote       [7 u/parse-float]   ; Quote asset volume
                 :trades      [8]                 ; Number of trades
                 :buy-volume  [9 u/parse-float]   ; Taker buy base asset volume
                 :buy-quote   [10 u/parse-float]  ; Taker buy quote asset volume
                 })

(defn get-stream
  "Convert pair to stream topic name"
  [type pair] (str (lower-pair pair) (type stream-types)))

(defn open-stream
  ([url stream]
   (->> (str url "/ws/" stream) http/websocket-client deref))
  ([url stream & streams]
   (->> (cons stream streams)
        (clojure.string/join "/")
        (u/url-encode-params (str url "/stream") :streams)
        http/websocket-client
        deref)))

(defn pair-stream [stream pair] (str (lower-pair pair) "@" stream))

(defn ws-query
  "Prepare websocket request"
  [& streams]
  (json/write-str {:id 1
                   :method "SUBSCRIBE"
                   :params (apply concat (for [[type pairs] (apply hash-map streams)]
                                           (map (partial get-stream type) pairs)))}))

(def rest-urls {:t "/api/v3/trades" :d "/api/v3/depth"})

(def spot-info-query (str spot-url "/v3/exchangeInfo"))
(def usdm-info-query (str usdm-url "/v1/exchangeInfo"))

(defn trades-rest-query
  "Prepare REST url request for trades"
  [pair]
  (str
   "https://www.binance.com/api/v3/trades?symbol="
   (upper-pair pair)
   "&limit=1000"))

(defn depth-rest-query
  "Prepare REST url for depth"
  [pair]
  (str
   "https://www.binance.com/api/v3/depth?symbol="
   (upper-pair pair)
   "&limit=1000"))

(defn candles-rest-query
  "Prepare REST url for candles query"
  [url pair interval & {:keys [start end limit]}]
  (u/url-encode-params
   url
   :symbol (de-hyphen pair)
   :interval (name interval)
   :startTime start
   :endTime end
   :limit limit))

(defn transform-trade-ws
  "Transform Binance trade record from websocket to Clickhouse row"
  [{;event-type "e"
    ;event-time "E"
    ;symbol "s"
    id "t"
    q "q"
    p "p"
    ;buyer-order-id "b"
    ;seller-order-id "a"
    time "T"
    buy "m"
    ;ignore "M"
    }]
  (let [price (Double/parseDouble p)
        quantity (Float/parseFloat q)]
    [id
     (new java.sql.Timestamp time)
     (if buy 0 1)
     price
     quantity
     (float (* price quantity))]))

(defn transform-trade
  "Transform Binance trade record from REST to Clickhouse row"
  [r] [(get r "id")
       (new java.sql.Timestamp (get r "time"))
       (if (get r "isBuyerMaker") 0 1)
       (Double/parseDouble (get r "price"))
       (Float/parseFloat (get r "qty"))
       (Float/parseFloat (get r "quoteQty"))])

(defn transform-depth-level
  "Transform Binance depth record to Clickhouse row"
  [time]
  (let [ts (new java.sql.Timestamp time)]
    (fn [[p q]]
      (let [price (Double/parseDouble p)]
        [ts
         price
         (-> q Float/parseFloat (* price) float)]))))

(defn transform-candle-rest
  "Transform candle record to further processing"
  [[t, o, h, l, c, v, _, qv, nt, bv, bqv]]
  [t
   [(Double/parseDouble o)
    (Double/parseDouble h)
    (Double/parseDouble l)
    (Double/parseDouble c)
    (Float/parseFloat v)
    (Float/parseFloat qv)
    nt
    (Float/parseFloat bv)
    (Float/parseFloat bqv)]])

(defn push-recent-trades!
  "Get recent trades from REST and put them by callback"
  [push-raw! trades-cache pairs]
  (doseq [pair pairs]
    (->> pair
         trades-rest-query
         u/http-get-json
         (map transform-trade)
         (push-raw! trades-cache pair))))

(defn transform-depths-rest [data]
  (into {} (for [[k v] data] [(str (first k)) v])))

(defn get-current-depths
  "Get depth from REST"
  [pairs]
  (for [pair pairs]
    [pair
     (->> pair
          depth-rest-query
          u/http-get-json
          transform-depths-rest)]))

(defn get-current-spreads
  [pairs]
  (for [[pair {a "a" b "b"}] (get-current-depths pairs)
        :let [[a b] (map (comp #(Double/parseDouble %) ffirst) [a b])]]
    [pair (/ (- a b) 0.5 (+ a b))]))

(defn get-candles
  "Get candles by REST"
  [url pair interval & {:keys [start end limit ts] :or {ts transform-candle-rest}}]
  (->> (candles-rest-query url pair interval :start start :end end :limit limit)
       u/http-get-json
       (map ts)))

(defn parse-topic
  [topic]
  (let [items (re-find #"^(\w+)@(\w+)$" topic)]
    (if (= (count items) 3) (rest items) nil)))

(defn push-ws-depth!
  [push-raw! {sell :s buy :b}
   pair
   {;type "e"
    time "E"
    ;symbol "s"
    ;first-id "U"
    ;last-id "u"
    bid "b"
    ask "a"}]
  (let [td (transform-depth-level time)]
    (push-raw! sell pair (map td ask))
    (push-raw! buy pair (map td bid))))

(defn mix-depth
  [pair snapshot data]
  (let [ss (@snapshot pair)]
    (if ss
      (let [{last-id "l"} ss
            {u2 "u"} data]
        (if (<= u2 last-id)
          data
          (let [{ask "a" bid "b" _u1 "U" _time "E"} data
                {ss-ask "a" ss-bid "b"} ss
                left (count @snapshot)]
            (debug "mix-depth: pair" pair "time" (u/ts-str _time) "last" last-id "U" _u1 "u" u2 "left" left)
            (if (== 1 left)
              (reset! snapshot nil)
              (swap! snapshot dissoc pair))
            (assoc data
                   "a" (concat ss-ask ask)
                   "b" (concat ss-bid bid)))))
      data)))

(defn all-pairs
  [url]
  (->> url
       u/http-get-json
       w/keywordize-keys
       :symbols
       (filter (comp (partial = "TRADING") :status))
       (map #(str (:baseAsset %) "-" (:quoteAsset %)))))


; USDM Futures

(defn signature
  [secret-key & data]
  (let [data (conj (into [] data)
                   :recvWindow (str 5000)
                   :timestamp (str (u/now-ts)))
        hmac (Mac/getInstance "HmacSHA256")
        sec-key (SecretKeySpec. (.getBytes secret-key), "HmacSHA256")]
    (.init hmac sec-key)
    (conj data :signature (->> data u/encode-params .getBytes (.doFinal hmac) Hex/encodeHex String.))))

(defn usdm-signed-request
  [verb url {:keys [api-key secret-key]} & params]
  (u/http-request-json verb (str usdm-url url)
                       :headers {"Content-Type" "application/x-www-form-urlencoded"
                                 "X-MBX-APIKEY" api-key}
                       :params (apply signature secret-key params)))

(def usdm-signed-get     (partial usdm-signed-request http/get))
(def usdm-signed-post    (partial usdm-signed-request http/post))

(def usdm-all-pairs      (partial all-pairs (str usdm-url "/v1/exchangeInfo")))

(def usdm-balance        (partial usdm-signed-get "/v2/balance"))
(def usdm-position-mode  (partial usdm-signed-get "/v1/positionSide/dual"))
(def usdm-income-history (partial usdm-signed-get "/v1/income"))
(def usdm-positions      (partial usdm-signed-get "/v2/positionRisk"))

(defn usdm-all-orders
  [keys symbol]
  (usdm-signed-get "/v1/allOrders" keys :symbol (de-hyphen symbol)))

(def order-types {:limit "LIMIT" :market "MARKET"})
(def order-sides {:buy "BUY" :sell "SELL"})
(def time-in-force-map {:gtc "GTC"; (Good-Til-Canceled) orders are effective until they are executed or canceled.
                        :ioc "IOC"; (Immediate or Cancel) orders fills all or part of an order immediately and cancels the remaining part of the order.
                        :fok "FOK"; (Fill or Kill) orders fills all in its entirety, otherwise, the entire order will be cancelled.
                        :gtx "GTX"; Good Till Crossing (Post Only)
                        })

(defn usdm-new-order!
  [keys symbol type side & {:keys [time-in-force quantity price] :or {time-in-force :gtc}}]
  (apply usdm-signed-post "/v1/order" keys
         :symbol (de-hyphen symbol)
         :type (order-types type)
         :side (order-sides side)
         (concat
          (when time-in-force
            [:timeInForce (time-in-force-map time-in-force)])
          (when quantity
            [:quantity quantity])
          (when price
            [:price price]))))

(defn usdm-cancel-order!
  [keys symbol & {:keys [order-id client-order-id]}]
  (assert (or order-id client-order-id))
  (apply usdm-signed-request http/delete "/v1/order" keys
         :symbol (de-hyphen symbol)
         (concat
          (when order-id
            ["orderId" order-id])
          (when client-order-id
            ["origClientOrderId" client-order-id]))))


; Exchange record

(defrecord Binance [name intervals-map candles-limit raw candles]
  u/Exchange
  (get-all-pairs [_] (all-pairs spot-info-query))
  (gather-ws-loop! [{raw :raw} push-raw! _]
    (let [{pairs :pairs trades :t} raw
          pairs-map (zipmap (map lower-pair pairs) pairs)
          depth-snapshot (atom nil)
          ws (->> pairs
                  (map (juxt (partial get-stream :t) (partial get-stream :d)))
                  (apply concat)
                  (clojure.string/join "/")
                  (u/url-encode-params "wss://stream.binance.com:9443/stream" :streams)
                  http/websocket-client
                  deref)]
      (info "Websocket connected")
      (push-recent-trades! push-raw! trades pairs)
      (reset! depth-snapshot (into {} (get-current-depths pairs)))
      (while true (let [chunk (json/read-str @(s/take! ws)) ; null!
                        {stream "stream" data "data"} chunk
                        [pair-id topic] (parse-topic stream)
                        pair (pairs-map pair-id)]
                    (if (and pair topic)
                      (try
                        (case topic
                          "trade" (push-raw! trades pair [(transform-trade-ws data)])
                          "depth" (push-ws-depth!
                                   push-raw!
                                   raw pair
                                   (if @depth-snapshot
                                     (mix-depth pair depth-snapshot data)
                                     data))
                          (warn "Binance: unknown stream topic" stream))
                        (catch Exception e
                          (error "Сhunk processing exception. Stream" stream "data:\n" data)
                          (throw e)))
                      (warn "Binance: unknown stream pair" stream "pair was" pair-id))))))
  (get-candles [_ _ fields interval pair start end]
    (get-candles (str spot-url "/v3/klines") pair interval :start start :end end)))
(defrecord BinanceUSDM [name intervals-map candles-limit raw candles]
  u/Exchange
  (open-streams [_ streams]
    (apply open-stream usdm-ws-url streams))
  (stream-agg-trades [m pairs]
    (->> pairs (map (partial pair-stream "aggTrade")) (u/open-streams m) u/stream-seq!))
  (stream-agg-trades [m pairs fields]
    (->> pairs (u/stream-agg-trades m) (map (u/field-parser fields agg-trade-rec))))
  (stream-candles [m _ tf pairs fields]
    (assert (keyword? tf))
    (->> pairs
         (map (partial pair-stream (str "kline_" (clojure.core/name tf))))
         (u/open-streams m)
         u/stream-seq!
         (map (u/field-parser fields candle-ws-rec))))
  (get-all-pairs [_] (all-pairs usdm-info-query))
  (get-candles [_ kind fields interval pair start end]
    (get-candles (str usdm-url (case kind
                                 nil    "/v1/klines"
                                 :cont  "/v1/continuousKlines"
                                 :index "/v1/indexPriceKlines"
                                 :mark  "/v1/markPriceKlines"))
                 pair interval :start start :end end
                 :ts (if (empty? fields)
                       transform-candle-rest
                       (u/field-parser fields candle-rec)))))

(defn create
  "Create Binance instance"
  [kind]
  (case kind
    :spot (Binance. "Binance" (zipmap (map keyword binance-intervals) binance-intervals) binance-candles-limit nil nil)
    :usdm (BinanceUSDM. "Binance-USDM" (zipmap (map keyword binance-intervals) binance-intervals) binance-candles-limit nil nil)))
