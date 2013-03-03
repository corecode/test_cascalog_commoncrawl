(defproject test2 "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [cascalog "1.10.0"]
                 [cascalog-more-taps "0.3.0"]
                 [clj-json "0.5.1"]
                 [org.apache.hadoop/hadoop-core "1.0.3"]]
  :warn-on-reflection true
  :main test2.core)
