(ns exch.binance.core
  (:require [clojure.tools.logging :refer [debug info warn error]]
            [clojure.string :as str]
            [clojure.walk :as w]
            [aleph.http        :as http]
            [exch.utils        :as u]
            [exch.binance.defs :as d]))

(import javax.crypto.Mac)
(import javax.crypto.spec.SecretKeySpec)
(import org.apache.commons.codec.binary.Hex)


; Consts and records


; Functions

(defn de-hyphen
  "Remove hyphens from string"
  [item]
  (clojure.string/replace item "-" ""))

(def lower-pair (comp clojure.string/lower-case de-hyphen)) ; Convert pair to lower name
(def upper-pair (comp clojure.string/upper-case de-hyphen)) ; Convert pair to upper name

(defn open-stream  [url stream]  (->> (str url "/ws/" stream) u/ws-client))
(defn open-streams [url streams] (->> streams (clojure.string/join "/") (u/ws-client (str url "/stream") :streams)))

(defn field-parser
  [fields rec pairs]
  (comp (->> pairs
             (zipmap (map de-hyphen pairs))
             (conj (:symbol rec))
             (assoc rec :pair)
             (u/field-parser fields)) #(get % "data")))

(defn pair-stream
  ([stream pair]
   (str (lower-pair pair) "@" stream))
  ([rec stream exch pairs fields]
   (->> pairs (map (partial pair-stream stream)) (u/open-streams exch) u/stream-seq! (map (field-parser fields rec pairs)))))

(def rest-urls {:t "/api/v3/trades" :d "/api/v3/depth"})


; Legacy

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
  [url pair interval & {:keys [start end limit ts]}]
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

(defn trading-symbols
  [url]
  (->> url
       u/http-get-json
       w/keywordize-keys
       :symbols
       (filter (comp (partial = "TRADING") :status))))

(defn all-pairs
  [url]
  (->> url
       trading-symbols
       (map #(str (:baseAsset %) "-" (:quoteAsset %)))))

(defn usdm-pair
  [base quote contract-type]
  (->> (case contract-type
         "CURRENT_QUARTER" "-CQ"
         "NEXT_QUARTER"    "-NQ"
         "PERPETUAL"       ""
         "" ""
         (throw (Exception. (str "Binance: unknown contract type " contract-type))))
       (str base "-" quote)))

(defn symbol-pair-map
  [url]
  (into {}
        (for [{base :baseAsset quote :quoteAsset symbol :symbol ct :contractType} (trading-symbols url)]
          [symbol (usdm-pair base quote ct)])))

; Signed functions

(defn signature
  [secret-key & data]
  (let [data (conj (into [] data)
                   :recvWindow (str 5000)
                   :timestamp (str (u/now-ts)))
        hmac (Mac/getInstance "HmacSHA256")
        sec-key (SecretKeySpec. (.getBytes secret-key), "HmacSHA256")]
    (.init hmac sec-key)
    (conj data :signature (->> data u/encode-params .getBytes (.doFinal hmac) Hex/encodeHex String.))))

(defn signed-request
  [verb url {:keys [api-key secret-key]} & params]
  (u/http-request-json verb url
                       :headers {"Content-Type" "application/x-www-form-urlencoded"
                                 "X-MBX-APIKEY" api-key}
                       :params (apply signature secret-key params)))

(defn usdm-signed-get  [url & args]   (apply signed-request http/get  (str d/usdm-url url) args))
(defn usdm-signed-post [url & args]   (apply signed-request http/post (str d/usdm-url url) args))
(defn spot-signed-get  [url & args]   (apply signed-request http/get  (str d/spot-url url) args))
(defn spot-signed-post [url & args]   (apply signed-request http/post (str d/spot-url url) args))

(def usdm-all-pairs      (partial all-pairs (str d/usdm-url "/v1/exchangeInfo")))

(def usdm-position-mode  (partial usdm-signed-get "/v1/positionSide/dual"))
(def usdm-income-history (partial usdm-signed-get "/v1/income"))

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
(def margin-types {:isolated "ISOLATED" :crossed "CROSSED"})

(defn usdm-cancel-order!
  [keys symbol & {:keys [order-id client-order-id]}]
  (assert (or order-id client-order-id))
  (apply signed-request http/delete (str d/usdm-url "/v1/order") keys
         :symbol (de-hyphen symbol)
         (concat
          (when order-id
            ["orderId" order-id])
          (when client-order-id
            ["origClientOrderId" client-order-id]))))


; Exchange records

(defrecord Binance [name intervals-map candles-limit raw candles]
  u/Exchange
  ; WS streams
  (open-streams [_ streams]
    (open-streams d/spot-ws-url streams))
  (trade-stream [this pairs fields]
    (pair-stream d/trade-ws-rec "trade" this pairs fields))
  (agg-trade-stream [this pairs fields]
    (pair-stream d/agg-trade-ws-rec "aggTrade" this pairs fields))
  (gather-ws-loop! [this push-raw! _]
    (let [{pairs :pairs trades :t} raw
          pairs-map (zipmap (map lower-pair pairs) pairs)
          depth-snapshot (atom nil)
          ws (do (assert (not-empty pairs))
                 (->> pairs
                      (map (juxt (partial pair-stream "trade") (partial pair-stream "depth")))
                      (apply concat)
                      (u/open-streams this)))]
      (info "Websocket connected")
      (push-recent-trades! push-raw! trades pairs)
      (reset! depth-snapshot (into {} (get-current-depths pairs)))
      (doseq [chunk (u/stream-seq! ws)
              :let [{stream "stream" data "data"} chunk
                    [pair-id topic] (parse-topic stream)
                    pair (pairs-map pair-id)]]
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
          (warn "Binance: unknown stream pair" stream "pair was" pair-id)))))
  ; REST
  (get-all-pairs [_] (all-pairs d/spot-info-query))
  (get-candles [_ _ fields interval pair start end]
    (get-candles (str d/spot-url "/v3/klines") pair interval :start start :end end :ts (u/field-parser fields d/candle-rec)))
  (order-ticker [_ pair fields]
    (-> (str d/usdm-url "/v3/ticker/bookTicker") (u/http-get-json :params [:symbol (de-hyphen pair)]) ((u/field-parser fields d/order-ticker-rec))))
  (get-rec [_ kind] (kind d/recs))
  (get-balance [_ acc-keys fields]
    (->> (get (spot-signed-get "/v3/account" acc-keys) "balances")
         (map (comp (juxt first rest)
                    (u/field-parser (cons :asset fields) d/spot-balance-rec)))
         (into {}))))

(defrecord BinanceUSDM [name intervals-map symbol-pair-map candles-limit raw candles]
  u/Exchange
  ; WS streams
  (open-streams [_ streams]
    (open-streams d/usdm-ws-url streams))
  (agg-trade-stream [this pairs fields]
    (pair-stream d/agg-trade-ws-rec "aggTrade" this pairs fields))
  (candle-stream [this _ tf pairs fields]
    (assert (keyword? tf))
    (pair-stream d/candle-ws-rec (str "kline_" (clojure.core/name tf)) this pairs fields))
  ; REST
  (info' [_ {:keys [assets pairs]}]
    (let [data (u/http-get-json d/usdm-info-url)
          result (remove nil? [(when assets
                                 (map (u/field-parser assets d/usdm-info-asset-rec) (data "assets")))
                               (when pairs
                                 (into {} (map
                                           (comp
                                            (juxt (comp (partial apply usdm-pair) first) second)
                                            (partial split-at 3)
                                            (u/field-parser (concat [:base-asset :quote-asset :contract-type] pairs) d/usdm-info-symbol-rec))
                                           (data "symbols"))))])]
      (if (-> result count (= 1)) (first result) result)))
  (get-all-pairs [_] (all-pairs d/usdm-info-url))
  (get-candles [_ kind fields interval pair start end]
    (get-candles (str d/usdm-url (case kind
                                   nil    "/v1/klines"
                                   :cont  "/v1/continuousKlines"
                                   :index "/v1/indexPriceKlines"
                                   :mark  "/v1/markPriceKlines"))
                 pair interval :start start :end end
                 :ts (u/field-parser fields d/candle-rec)))
  (order-ticker [_ pair fields]
    (-> (str d/usdm-url "/v1/ticker/bookTicker") (u/http-get-json :params [:symbol (de-hyphen pair)]) ((u/field-parser fields d/order-ticker-rec))))
  (get-rec [_ kind] (kind d/recs))
  (get-balance [_ acc-keys fields]
    (->> (usdm-signed-get "/v2/balance" acc-keys)
         (map (comp (juxt first rest)
                    (u/field-parser (cons :asset fields) d/future-balance-rec)))
         (into {})))
  (get-positions [_ acc-keys fields]
    (->> (usdm-signed-get "/v2/positionRisk" acc-keys)
         (map (comp (juxt (comp @symbol-pair-map first) rest)
                    (u/field-parser (cons :symbol fields) d/usdm-position-rec)))
         (into {})))
  (place-order!'
    [_ keys pair type side {:keys [time-in-force quantity price close-position client-order-id resp-type fields] :or {time-in-force :gtc}}]
    (let [result (apply usdm-signed-post "/v1/order" keys
                        :symbol (de-hyphen pair)
                        :type (type order-types)
                        :side (side order-sides)
                        (concat
                         (when (and time-in-force (not= type :market))
                           [:timeInForce (time-in-force-map time-in-force)])
                         (when close-position
                           [:closePosition close-position])
                         (when client-order-id
                           [:newClientOrderId client-order-id])
                         (when quantity
                           [:quantity quantity])
                         (when price
                           [:price price])
                         (when resp-type
                           [:newOrderRespType (resp-type {:ack "ACK" :result "RESULT"})])))]
      (if fields
        ((u/field-parser fields d/usdm-order-resp-rec) result)
        result)))
  (setup!'
    [_ keys pair {:keys [margin-type leverage]}]
    (when leverage
      (assert (and (int? leverage) (pos? leverage)))
      (usdm-signed-post "/v1/leverage" keys :symbol (de-hyphen pair) :leverage leverage))
    (when margin-type
      (usdm-signed-post "/v1/marginType" keys :symbol (de-hyphen pair) :marginType (margin-type margin-types)))))

(defn create
  "Create Binance instance"
  [kind]
  (case kind
    :spot (Binance.     "Binance"      (zipmap (map keyword d/binance-intervals) d/binance-intervals) d/binance-candles-limit nil nil)
    :usdm (BinanceUSDM. "Binance-USDM" (zipmap (map keyword d/binance-intervals) d/binance-intervals) (delay (symbol-pair-map d/usdm-info-url)) d/binance-candles-limit nil nil)))
