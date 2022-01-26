(defproject org.clojars.i-s/exch.clj "0.1.0-SNAPSHOT"
  :description "Exhange access library"
  :url "https://github.com/i-v-s/exch.clj"
  :license {:name "GNU General Public License v3.0"
            :url "https://www.gnu.org/licenses/gpl-3.0.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "1.2.1"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/core.async "1.3.610"]
                 [commons-codec/commons-codec "1.15"]
                 [aleph "0.4.7-alpha7"]]
  :main ^:skip-aot exch.core
  :repl-options {:init-ns exch.core}
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]])
