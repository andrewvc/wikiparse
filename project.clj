(defproject wikiparse "0.2.0"
  :description "Import Wikipedia data into elasticsearch"
  :url "http://example.com/FIXME"
  :main wikiparse.core
  :jvm-opts ["-Xmx1g" "-server"]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.xml "0.0.7"]
                 [org.clojure/tools.cli "0.2.4"]
                 [clojurewerkz/elastisch "1.3.0-beta1"]
                 [org.apache.commons/commons-compress "1.5"]])
