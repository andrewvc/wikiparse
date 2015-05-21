(defproject wikiparse "0.2.5"
  :description "Import Wikipedia data into elasticsearch"
  :url "http://example.com/FIXME"
  :aot [wikiparse.core]
  :main wikiparse.core
  :jvm-opts ["-Xmx2g" "-server"]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.cli "0.2.4"]
                 [clojurewerkz/elastisch "2.2.0-beta3"]
                 [org.apache.commons/commons-compress "1.5"]])
