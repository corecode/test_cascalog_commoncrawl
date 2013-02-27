(ns test2.core
  (:require [clojure.string :as s]
            [cascalog.ops :as c]
            [cascalog.tap :as tap]
            [cascalog.workflow :as w])
  (:use cascalog.api
        [cascalog.more-taps :only (hfs-wrtseqfile)])
  (:import [org.apache.hadoop.io Text]
           [org.apache.commons.httpclient URI]
           [cascading.tuple Fields]
           [cascading.scheme.hadoop TextLine TextLine$Compress])
  (:gen-class))

(defn make-metadata-tap
  "Creates tap for common crawl metadata files."
  [path]
  (hfs-wrtseqfile path Text Text :outfields ["key" "value"]))

(defn hfs-textline-compressed
  [path & opts]
  (let [scheme (->> (:outfields (apply array-map opts) Fields/ALL)
                    (w/text-line ["line"]))]
    (.setSinkCompression scheme TextLine$Compress/ENABLE)
    (apply tap/hfs-tap scheme path opts)))

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
                        (hfs-textline-compressed outpath))
           trap-tap (if (= outpath "-")
                      (stdout)
                      (hfs-seqfile (str outpath ".trap")))]
       (with-job-conf {"mapred.output.compress" "true"}
         (?- output-tap
             (query-domains metadata-tap trap-tap))))))
