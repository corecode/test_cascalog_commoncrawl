(defproject test2 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascalog "1.10.0"]
                 [cascalog-more-taps "0.3.0"]
                 [commons-httpclient "3.0.1"]]
  :profiles {:dev
             {:dependencies [[org.apache.hadoop/hadoop-core "1.1.1"]]}}
  :warn-on-reflection true
  :main test2.core)
