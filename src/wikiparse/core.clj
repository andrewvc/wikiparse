(ns wikiparse.core
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as es-index]
            [clojurewerkz.elastisch.rest.bulk :as es-bulk])
  (:import (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (java.util.concurrent.atomic AtomicLong)))

(defn bz2-reader
  "Returns a streaming Reader for the given compressed BZip2
  file. Use within (with-open)."
  [filename]
  (-> filename io/file io/input-stream BZip2CompressorInputStream. io/reader))

;; XML Mapping functions + helpers

(defn elem->map
  "Turns a list of elements into a map keyed by tag name. This doesn't
   work so well if tag names are repeated"
  [mappers]
  (fn [elems]
    (reduce (fn [m elem]
              (if-let [mapper ((:tag elem) mappers)]
                (assoc m (:tag elem) (mapper elem))
                m))
            {}
            elems)))

(def text-mapper (comp first :content))

(def int-mapper #(Integer/parseInt (text-mapper %)))

(def revision-mapper
  (comp 
   (elem->map 
    {:text text-mapper
     :timestamp text-mapper
     :format (comp keyword text-mapper)})
   :content))

(def page-mappers
  {:title text-mapper
   :ns int-mapper
   :id int-mapper
   :redirect text-mapper
   :revision revision-mapper})

;; Parse logic

(defn match-tag
  "match an element by tag name"
  [tag-name]
  #(= tag-name (:tag %)))

(defn filter-page-elems
  [wikimedia-elems]
  (filter (match-tag :page) wikimedia-elems))

(defn xml->pages
  [parsed]
  (map (comp (elem->map page-mappers) :content)
       (filter-page-elems (:content parsed))))

;; Elasticsearch indexing

(defn bulk-index-pages
  [pages]
  (es-bulk/bulk-with-index-and-type
          "en-wikipedia" "page"
          (es-bulk/bulk-index pages)))

(defn es-format-pages
  [pages]
  (map (fn [{id :id :as page}] (dissoc (assoc page :_id id) :id)) pages))

(defn index-pages
  [pages callback]
  (map (fn [ppart]
         (bulk-index-pages ppart)
         (callback ppart))
       (partition-all 1000 pages)))

;; Bootstrap

(def page-mapping
  {
   :properties
    {
     :title {
             :type :multi_field
             :fields 
             {
              :title_snow {:type :string :analyzer :snowball}
              :title_exact {:type :string :index :not_analyzed}
              }
             }
     :ns {:type :string :index :not_analyzed}
     :redirect {:type :string :index :not_analyzed}
     :text {
            :type :multi_field
            :fields 
            {
             :text_snow {:type :string :analyzer :snowball}
             :text_exact {:type :string :index :not_analyzed}
             }
            }
     }
   }
)

(defn ensure-index
  [name]
  (when (not (es-index/exists? name))
    (println (format "Creating index %s" name))
    (es-index/create name :mappings {:page page-mapping})))

(defn index-dump
  [path callback]
  (-> (bz2-reader path)
      (xml/parse)
      (xml->pages)
      (es-format-pages)
      (index-pages callback)))

(defn -main
  [path es-url & args]
  (esr/connect! es-url)
  (ensure-index "en-wikipedia")
  (let [counter (AtomicLong.)
        callback (fn [pages] (println (format "+ %s pages" (.addAndGet counter (count pages)))))]
    (dorun (index-dump path callback))
    (println (format "Indexed %s pages" (.get counter)))))
