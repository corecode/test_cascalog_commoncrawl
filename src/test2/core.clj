(ns test2.core
  (:require [clojure.string :as s]
            [cascalog.ops :as c]
            [cascalog.tap :as tap]
            [cascalog.workflow :as w]
            [clj-json.core :as json])
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
  (some-> uri str URI. .getHost s/lower-case))

(defmapop ^String extract-domain
  "Return the domain part of a host name"
  [^String domain]
  (->> domain (re-matches #"^(?:[^.]+[.])*?([a-z0-9-]+[.](?:com?[.])?[a-z-]+)$") peek))

(defmapcatop extract-links
  "Return the links from metadata."
  [^Text metadata]
  (let [json (-> metadata str json/parse-string)
        links (get-in json ["content" "links"] [])]
    (->> links
         (keep (fn [x]
                 (or (and (= "a" (get x "type"))
                          (get x "href"))
                     nil)))
         vec)))

(defn -union
  "Faster version of union."
  [a b]
  (cond
   (= 1 (count b)) (conj a (first b))
   (= 1 (count a)) (conj b (first a))
   :else (set (concat a b))))

(defparallelagg collect-in-set
  "Aggregates values in a set."
  :init-var #'hash-set
  :combine-var #'-union)

(defn list-to-str
  [l]
  (s/join " " l))

(defn query-domains
  "Extract unique domain names from METADATA-TAP."
  [metadata-tap trap-tap]
  (<- [?link-domain ?domain-list-str]
      (metadata-tap :> ?url ?meta)
      (extract-links ?meta :> ?link)
      (parse-host ?url :> ?host)
      (extract-domain ?host :> ?domain)
      (parse-host ?link :> ?link-host)
      (extract-domain ?link-host :> ?link-domain)
      (not= ?link-domain ?domain)
      (collect-in-set ?domain :> ?domain-list)
      (list-to-str ?domain-list :> ?domain-list-str)
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
                      (hfs-textline (str outpath ".trap")))]
       (?- output-tap
           (query-domains metadata-tap trap-tap)))))
