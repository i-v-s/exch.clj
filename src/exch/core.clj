(ns exch.core
  (:require [exch.utils :as u]
            [exch.exmo :as exm]
            [exch.binance.core :as bnb]))


(defn -main
  [command & args]
  (println command args))
