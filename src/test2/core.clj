(ns test2.core
  (:require [clojure.string :as s]
            [cascalog.ops :as c])
  (:use cascalog.api
        [cascalog.more-taps :only (hfs-wrtseqfile)])
  (:import [org.apache.hadoop.io Text]
           [org.apache.commons.httpclient URI])
  (:gen-class))

(defn make-metadata-tap
  "Creates tap for common crawl metadata files."
  [path]
  (hfs-wrtseqfile path Text Text :outfields ["key" "value"]))

(defmapop ^String parse-host
  "Return the host part of an URI."
  [^Text uri]
  (-> uri str URI. .getHost (or "")))

(defmapop ^String extract-domain
  "Return the domain part of a host name"
  [^String domain]
  (->> domain (re-matches #"^(?:[^.]+[.])*?([^.]+[.](?:com?[.])?[^.]+)$") peek))

(defn query-domains
  "Extract unique domain names from METADATA-TAP."
  [metadata-tap trap-tap]
  (<- [?domain]
      (metadata-tap :> ?url _)
      (parse-host ?url :> ?host)
      (extract-domain ?host :> ?domain)
      (:distinct true)
      (:trap trap-tap)))

(defn -main
  ([inpath] (-main inpath "-"))
  ([inpath outpath]
     (let [metadata-tap (make-metadata-tap inpath)
           output-tap (if (= outpath "-")
                        (stdout)
                        (hfs-textline outpath))
           trap-tap (if (= outpath "-")
                      (stdout)
                      (hfs-seqfile (str outpath ".trap")))]
       (?- output-tap
           (query-domains metadata-tap trap-tap)))))
