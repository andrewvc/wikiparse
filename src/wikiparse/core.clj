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

(def connection (atom nil))

(defn tune-performance
  [conn name]
  (println "Tuning index performance")
  (es-index/update-settings conn name {:index
                                       {
                                        :refresh_interval 60
                                        :gateway {:local {:sync "60s"}}
                                        :translog {
                                                   :interval "60s"
                                                   :flush_threshold_size "756mb"
                                                   }
                                        } } ))

(defn connect!
  "Connect once to the ES cluster"
  [es]
  (swap! connection
         (fn [con]
           (if-not con
             (esr/connect (or es "http://localhost:9200"))
             con))))

(defmacro with-connection
  [es bind & body]
  `(let [~bind (connect! ~es)]
     ~@body))

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
  {:title    text-mapper
   :ns       int-mapper
   :id       int-mapper
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

(defn es-page-formatter-for
  "returns an fn that formats the page as a bulk action tuple for a given index"
  [index-name]
  (fn 
    [{title :title redirect :redirect {text :text timestamp :timestamp format :format} :revision :as page}]
    ;; the target-title ensures that redirects are filed under the article they are redirects for
    (let [target-title (or redirect title)]
      [{:update {:_id (string/lower-case target-title) :_index index-name :_type :page}}
       {:script "if (is_redirect) {ctx._source.redirects += redirect};
                   ctx._source.suggest.input += title;
                 if (!is_redirect) {
                   ctx._source.title = title;
                   ctx._source.timestamp = timestamp;
                   ctx._source.format = format;
                   ctx._source.body = body;
                 };"
        :params {:redirect     title, :title title, :timestamp timestamp, :format format,
                 :target_title target-title, :body text
                 :is_redirect  (boolean redirect)}
        :upsert (merge {:title     target-title
                        :redirects (if redirect [title] [])
                        :suggest   {:input [title] :output target-title}
                        } (when (not redirect) {:body text :timestamp timestamp :format format}))
        }])))

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

(defn bulk-index-pages
  [conn pages]
  ;; unnest command / doc tuples with apply concat
  (let [resp (es-bulk/bulk conn (apply concat pages) :consistency "one")]
    (when ((comp not = :ok) resp)
      (println resp))))

(defn index-pages
  [pages conn callback]
  (bulk-index-pages conn pages)
  (callback pages))

(def page-mapping
  {
   :_all {:_enabled false}
   :properties
    {
     :ns {:type :string :index :not_analyzed}
     :redirect {:type :string :index :not_analyzed}
     :title {
             :type :string
             :fields
             {
              :title_snow {:type :string :analyzer :snowball}
              :title_simple {:type :string :analyzer :simple}
              :title_exact {:type :string :index :not_analyzed}}}
     :redirects {
             :type :string
             :fields
             {
              :redirects_snow {:type :string :analyzer :snowball}
              :redirects_simple {:type :string :analyzer :simple}
              :redirects_exact {:type :string :index :not_analyzed}}}
     :body {
             :type :string
             :fields
             {
              :body_snow {:type :string :analyzer :snowball}
              :body_simple {:type :string :analyzer :simple}}}
     :suggest {
               :type :completion
               :index_analyzer :simple
               :search_analyzer :simple}
     :timestamp {:type :date}
     :format {:type :string :index :not_analyzed}
     }
   })

;; Bootstrap + Run

(defn ensure-index
  [conn name]
  (when (not (es-index/exists? conn name))
    (println (format "Deleting index %s" name))
    (es-index/delete conn name)
    (println (format "Creating index %s" name))
    (es-index/create conn name :settings {:index {:number_of_shards 1, :number_of_replicas 0}} :mappings {:page page-mapping})))

(defn index-dump
  [rdr conn callback phase index-name]
  ;; Get a reader for the bz2 file
  (dorun (pmap
           (fn [pages]
             (-> pages
                 (filter-pages phase)
                 (es-format-pages index-name)
                 (index-pages conn callback))
             )
           (partition-all 25000 (xml->pages (xml/parse rdr))))))

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
    (with-connection (:es opts) conn
                     (ensure-index conn (:index opts))
                     (tune-performance conn (:index opts))
                     (let [counter (AtomicLong.)
                           callback (fn [pages] (println (format "@ %s pages" (.addAndGet counter (count pages)))))]
                       (doseq [phase [:redirects :full]]
                         (with-open [rdr (bz2-reader path)]
                           (println (str "Processing " phase))
                           (dorun (index-dump rdr conn callback phase (:index opts)))))
                       (println (format "Indexed %s pages" (.get counter))))
                     )
    (System/exit 0)
    ))