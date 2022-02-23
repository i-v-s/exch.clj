(ns exch.binance.defs
  (:require [clojure.string :as str]
            [exch.utils     :as u]))


(def binance-intervals
  "Chart intervals: (m)inutes, (h)ours, (d)ays, (w)eeks, (M)onths"
  ["1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d" "3d" "1w" "1M"])

(def binance-candles-limit 500)

(def spot-url "https://www.binance.com/api")
(def spot-ws-url "wss://stream.binance.com:9443")
(def usdm-url "https://fapi.binance.com/fapi")
(def usdm-ws-url "wss://fstream.binance.com")

(def price-filter-rec {:type ["filterType"] ; "PRICE_FILTER",
                       :min  ["minPrice" u/parse-double']
                       :max  ["maxPrice" u/parse-double']
                       :step ["tickSize" u/parse-double']})

(def lot-filter-rec  {:type ["filterType"]
                      :min  ["minQty"   u/parse-double']
                      :max  ["maxQty"   u/parse-double']
                      :step ["stepSize" u/parse-double']})

(def lot-filter-parser (u/field-parser [:min :max :step] lot-filter-rec))


;**********************************************************************************************************************
;******************************************* USDM Market data endpoints ***********************************************
;**********************************************************************************************************************

;; Exchange Information
;; GET /fapi/v1/exchangeInfo
;; Current exchange trading rules and symbol information

(def usdm-info-url (str usdm-url "/v1/exchangeInfo"))
(def usdm-info-asset-rec {:asseet    ["asset"]
                          :margin?   ["marginAvailable"]   ; whether the asset can be used as margin in Multi-Assets mode
                          :auto-exch ["autoAssetExchange"] ; auto-exchange threshold in Multi-Assets margin mode
                          })
(def usdm-info-symbol-rec {:symbol               ["symbol"]
                           :pair                 ["pair"]
                           :contract-type        ["contractType"] ; "PERPETUAL",
                           :delivery-ts          ["deliveryDate"]
                           :onboard-ts           ["onboardDate"]
                           :status               ["status"]
                           :base-asset           ["baseAsset"]
                           :quote-asset          ["quoteAsset"]
                           :margin-asset         ["marginAsset"]
                           :price-precision      ["pricePrecision"]    ; please do not use it as tickSize
                           :quantity-precision   ["quantityPrecision"] ; please do not use it as stepSize
                           :base-asset-precision ["baseAssetPrecision"]
                           :quote-precision      ["quotePrecision"]
                           :underlying-type      ["underlyingType"]
                           :underlying-sub-type  ["underlyingSubType"]
                           :settle-plan          ["settlePlan"]
                           :trigger-protect      ["triggerProtect" u/parse-float] ; threshold for algo order with "priceProtect"
                           :filters              ["filters"]
                           :market-lot           ["filters" (partial some #(and (= (% "filterType") "MARKET_LOT_SIZE") %)) lot-filter-parser]
                           :price-step           ["filters" (partial some #(and (= (% "filterType") "PRICE_FILTER") %)) "tickSize" u/parse-double']
                           :order-types          ["OrderType"]
                           :time-in-force        ["timeInForce"]
                           :liquidation-fee      ["liquidationFee"  u/parse-float]   ; liquidation fee rate
                           :market-take-bound    ["marketTakeBound" u/parse-double'] ; the max price difference rate( from mark price) a market order can make
                           })

(def base-ws-rec (partial assoc {:event-type ["e"]
                                 :event-ts   ["E"]
                                 :symbol     ["s"]}))

(def trade-ws-rec (base-ws-rec
                   :id        ["t"] ; Trade ID
                   :price     ["p" u/parse-double']
                   :quantity  ["q" u/parse-float]
                   :buyer-id  ["b"]
                   :seller-id ["a"]
                   :trade-ts  ["T"]
                   :buy?      ["m"]))

(def agg-trade-ws-rec (base-ws-rec
                       :agg-trade-id ["a"]
                       :price        ["p" u/parse-double']
                       :quantity     ["q" u/parse-float]
                       :first-id     ["f"]
                       :last-id      ["l"]
                       :trade-ts     ["T"]
                       :trade-sql-ts ["T" u/sql-ts]
                       :trade-str-ts ["T" u/sql-ts str]
                       :buy?         ["m"]))

(def depth-level-rec {:price    [first u/parse-double']
                      :quantity [second u/parse-float]})

(def depth-ws-rec (let [pq-map (partial map (u/field-parser [:price :quantity] depth-level-rec))]
                    (base-ws-rec
                     :first-id ["U"] ; First update ID in event
                     :final-id ["u"] ; Final update ID in event
                     :bids-pq  ["b" pq-map] ; Bids to be updated
                     :asks-pq  ["a" pq-map] ; Asks to be updated
                     )))

(def candle-ws-rec (base-ws-rec
                    :open-ts      ["k" "t"]
                    :open-ts-sql  ["k" "t" u/sql-ts]
                    :open-ts-str  ["k" "t" u/sql-ts str]
                    :close-ts     ["k" "T"]
                    :close-ts-sql ["k" "T" u/sql-ts]
                    :close-ts-str ["k" "T" u/sql-ts str]
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
                    ))

(def candle-rec {:open-ts      [0]                 ; Open time
                 :open-ts-sql  [0 u/sql-ts]
                 :open-ts-str  [0 u/sql-ts str]
                 :open-ts-ins  [0 u/ts-to-instant]
                 :open         [1 u/parse-double'] ; Open
                 :high         [2 u/parse-double'] ; High
                 :low          [3 u/parse-double'] ; Low
                 :close        [4 u/parse-double'] ; Close
                 :volume       [5 u/parse-float]   ; Volume
                 :close-ts     [6]                 ; Close time
                 :close-ts-sql [6 u/sql-ts]        ; Close time
                 :close-ts-str [6 u/sql-ts str]    ; Close time
                 :close-ts-ins [6 u/ts-to-instant] ; Close time
                 :quote        [7 u/parse-float]   ; Quote asset volume
                 :trades       [8]                 ; Number of trades
                 :buy-volume   [9 u/parse-float]   ; Taker buy base asset volume
                 :buy-quote    [10 u/parse-float]  ; Taker buy quote asset volume
                 })

(def order-ticker-rec {:ts         ["time"]
                       :symbol     ["symbol"]
                       :bid-price  ["bidPrice" u/parse-double']
                       :ask-price  ["askPrice" u/parse-double']
                       :bid-volume ["bidQty" u/parse-float]
                       :ask-volume ["askQty" u/parse-float]})

(def spot-balance-rec {:asset  ["asset"]
                       :free   ["free" u/parse-double']
                       :locked ["locked" u/parse-double']})

(def future-balance-rec {:alias             ["accountAlias"]
                         :asset             ["asset"]
                         :balance           ["balance"            u/parse-double'] ; wallet balance
                         :cross-balance     ["crossWalletBalance" u/parse-double'] ; crossed wallet balance
                         :cross-pnl         ["crossUnPnl"         u/parse-float]   ; unrealized profit of crossed positions
                         :free              ["availableBalance"   u/parse-double'] ; available balance
                         :max-withdraw      ["maxWithdrawAmount"  u/parse-double'] ; maximum amount for transfer out
                         :margin-available? ["marginAvailable"]                    ; whether the asset can be used as margin in Multi-Assets mode
                         :update-ts         ["updateTime"]})

;; Position Information V2 (USER_DATA)
;; GET /fapi/v2/positionRisk (HMAC SHA256)
;; Get current position information.

(def usdm-position-rec {:entry        ["entryPrice"       u/parse-double']
                        :margin-type  ["marginType"]      ; "isolated"
                        :auto-add?    ["isAutoAddMargin"] ; "false"
                        :isolated     ["isolatedMargin"   u/parse-double']
                        :leverage     ["leverage"         u/parse-int]
                        :liquidation  ["liquidationPrice" u/parse-double']
                        :mark         ["markPrice"        u/parse-double']
                        :max-notional ["maxNotionalValue" u/parse-double']
                        :amount       ["positionAmt"      u/parse-float]
                        :symbol       ["symbol"]
                        :profit       ["unRealizedProfit" u/parse-float]
                        :side         ["positionSide"] ; "BOTH"
                        :update-ts    ["updateTime"]})


;; New Order (TRADE)
;; POST /fapi/v1/order (HMAC SHA256)
;; Send in a new order.

(def usdm-order-resp-rec {:client-order-id ["clientOrderId"]
                          :cum-qty         ["cumQty" u/parse-float]
                          :cum-quote       ["cumQuote" u/parse-float]
                          :executed-qty    ["executedQty" u/parse-float]
                          :id              ["orderId" u/parse-int]
                          :avg-price       ["avgPrice" u/parse-double']
                          :original-qty    ["origQty" u/parse-float]
                          :price           ["price" u/parse-double']
                          :reduce-only     ["reduceOnly"]
                          :side            ["side" str/lower-case keyword]
                          :position-side   ["positionSide" str/lower-case keyword]
                          :status          ["status" str/lower-case keyword]
                          :stop-price      ["stopPrice" u/parse-double'] ; please ignore when order type is TRAILING_STOP_MARKET
                          :close-psition   ["closePosition"] ; if Close-All
                          :symbol          ["symbol"]
                          :time-in-force   ["timeInForce"] ; "GTC",
                          :type            ["type"] ; "TRAILING_STOP_MARKET",
                          :orig-type       ["origType"] ; "TRAILING_STOP_MARKET",
                          :activate-price  ["activatePrice" u/parse-double'] ; activation price, only return with TRAILING_STOP_MARKET order
                          :price-rate      ["priceRate" u/parse-float] ; callback rate, only return with TRAILING_STOP_MARKET order
                          :update-ts       ["updateTime"]
                          :working-type    ["workingType"] ; "CONTRACT_PRICE",
                          :price-protect   ["priceProtect"] ; if conditional order trigger is protected   
                          })


(def recs {:candle    candle-rec
           :candle-ws candle-ws-rec
           :trade-ws  trade-ws-rec})

(def spot-info-query (str spot-url "/v3/exchangeInfo"))
