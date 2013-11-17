(ns wikiparse.core
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as es-index]
            [clojurewerkz.elastisch.rest.bulk :as es-bulk])
  (:import (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (java.util.concurrent.atomic AtomicLong))
  (:gen-class))

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

(defn attr-mapper
  [attr]
  (fn [{attrs :attrs}]
    (get attrs attr)))

  

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
   :redirect (attr-mapper :title)
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
  (pmap (comp (elem->map page-mappers) :content)
       (filter-page-elems (:content parsed))))

;; Elasticsearch indexing

(defn bulk-index-pages
  [pages]
  ;; unnest command / doc tuples with apply concat
  (let [resp (es-bulk/bulk (apply concat pages) :consistency "one")]
    (when ((comp not = :ok) resp)
      (println resp))))

(defn es-page-formatter-for
  "returns an fn that formats the page as a bulk action tuple for a given index"
  [index-name]
  (fn 
    [{title :title redirect :redirect {text :text} :revision :as page}]
    ;; the target-title ensures that redirects are filed under the article they are redirects for
    (let [target-title (or redirect title)]
      [{:update {:_id (string/lower-case target-title) :_index index-name :_type :page}}
       {:script "if (is_redirect) {ctx._source.redirects += redirect};
               ctx._source.suggest.input += title;
               if (!is_redirect) { ctx._source.title = title; ctx._source.body = body};"
        :params {:redirect title, :title title 
                 :target_title target-title, :body text 
                 :is_redirect (boolean redirect)}
        :upsert {:title target-title
                 :redirects (if redirect [title] [])
                 :suggest {:input [title] :output target-title}
                 :body (when (not redirect) text)}}])))

(defn es-format-pages
  [pages index-name]
  (map (es-page-formatter-for index-name) pages))

(defn phase-filter
  [phase]
  (cond (= :redirects phase) :redirect
        (= :full phase) (comp nil? :redirect)
        :else nil))

(defn filter-pages
  [pages phase]
  (filter (phase-filter phase) (filter #(= 0 (:ns %)) pages)))

(defn index-pages
  [pages callback]
  (pmap (fn [ppart]
         (bulk-index-pages ppart)
         (callback ppart))
       (partition-all 100 pages)))

(def page-mapping
  {
   :properties
    {
     :ns {:type :string :index :not_analyzed}
     :redirect {:type :string :index :not_analyzed}
     :title {
             :type :multi_field
             :fields 
             {
              :title_snow {:type :string :analyzer :snowball}
              :title_simple {:type :string :analyzer :simple}
              :title_exact {:type :string :index :not_analyzed}}}
     :redirects {
             :type :multi_field
             :fields 
             {
              :redirects_snow {:type :string :analyzer :snowball}
              :redirects_simple {:type :string :analyzer :simple}
              :redirects_exact {:type :string :index :not_analyzed}}}
     :body {
             :type :multi_field
             :fields 
             {
              :body_snow {:type :string :analyzer :snowball}
              :body_simple {:type :string :analyzer :simple}}}
     :suggest {
               :type :completion
               :index_analyzer :simple
               :search_analyzer :simple}}})

;; Bootstrap + Run

(defn ensure-index
  [name]
  (when (not (es-index/exists? name))
    (println (format "Deleting index %s" name))
    (es-index/delete name)
    (println (format "Creating index %s" name))
    (es-index/create name :mappings {:page page-mapping})))

(defn index-dump
  [rdr callback phase index-name]
  ;; Get a reader for the bz2 file
  (-> rdr
      ;; Return a data.xml lazy parse-seq
      (xml/parse)       
      ;; turn the seq of elements into a seq of maps
      (xml->pages)      
      ;; Filter only ns 0 pages (only include 'normal' wikipedia articles)
      ;; Also, indicate whether we're processing redirects or full articles
      ;; in this pass. Redirects are indexed first, as re-tokenizing the full article
      ;; text with an update query is expensive. We want the article text to be the last
      ;; update
      (filter-pages phase)
      ;; re-map fields for elasticsearch
      (es-format-pages index-name) 
      ;; send the fully formatted fields to elasticsearch
      (index-pages callback)))

(defn parse-cmdline
  [args]
  (let [[opts args banner]
        (cli/cli args
           "Usage: wikiparse [switches] path_to_bz2_wiki_dump"
           ["-h" "--help" "display this help and exit"]
           ["--es" "elasticsearch connection string" :default "http://localhost:9200"]
           ["--index" "elasticsearch index name" :default "en-wikipedia"])]
    (when (or (empty? args) (:help opts))
      (println banner)
      (System/exit 1))
    [opts (first args)]))

(defn -main
  [& args]
  (let [[opts path] (parse-cmdline args)]
    (esr/connect! (:es opts)
    (ensure-index (:index opts))
    (let [counter (AtomicLong.)
          callback (fn [pages] (println (format "@ %s pages" (.addAndGet counter (count pages)))))]
      (doseq [phase [:redirects :full]]
        (with-open [rdr (bz2-reader path)]
          (println (str "Processing " phase))
          (dorun (index-dump rdr callback phase (:index opts)))))
            (println (format "Indexed %s pages" (.get counter))))
    (System/exit 0))))
