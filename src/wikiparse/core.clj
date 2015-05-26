(ns wikiparse.core
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [clojure.core.reducers :as r]
            [clojure.pprint :as pp]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as es-index]
            [clojurewerkz.elastisch.rest.bulk :as es-bulk])
  (:import (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (java.util.concurrent.atomic AtomicLong))
  (:gen-class))

(def connection (atom nil))

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
  (r/filter (match-tag :page) wikimedia-elems))

(defn xml->pages
  [parsed]
  (r/map (comp (elem->map page-mappers) :content)
        (filter-page-elems parsed)))

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
  (r/map (es-page-formatter-for index-name) pages))

(defn phase-filter
  [phase]
  (cond (= :simultaneous) identity
        (= :redirects phase) :redirect
        (= :full phase) (comp nil? :redirect)
        :else nil))

(defn filter-pages
  [pages phase]
  (r/filter (phase-filter phase) (r/filter #(= 0 (:ns %)) pages)))

(defn bulk-index-pages
  [conn pages]
  ;; unnest command / doc tuples with apply concat
  ;(pp/pprint pages)
  (let [resp (es-bulk/bulk conn pages :consistency "one")]
    (when ((comp not = :ok) resp)
      (println resp))))

(defn index-pages
  [bulk-lines conn callback]
  (bulk-index-pages conn bulk-lines)
  (callback bulk-lines))

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
    (es-index/create conn name
                     :settings {
                                :index {
                                        :number_of_shards 1,
                                        :number_of_replicas 0
                                        :refresh_interval 60
                                        :gateway {:local {:sync "60s"}}
                                        :translog {
                                                   :interval "60s"
                                                   :flush_threshold_size "756mb"
                                                   }
                                        }
                                }
                     :mappings {
                                :page page-mapping
                                })))

(defn index-dump
  [rdr conn callback phase index-name batch-size]
  ;; Get a reader for the bz2 file
  (dorun (pmap (fn [elems]
                 (-> elems
                     (xml->pages)
                     (filter-pages phase)
                     (es-format-pages index-name)
                     (r/flatten)
                     (r/foldcat)
                     (index-pages conn callback)))
                (partition-all batch-size (:content (xml/parse rdr))))))

(defn parse-cmdline
  [args]
  (let [[opts args banner]
        (cli/cli args
           "Usage: wikiparse [switches] path_to_bz2_wiki_dump"
           ["-h" "--help" "display this help and exit"]
           ["--es" "elasticsearch connection string" :default "http://localhost:9200"]
           ["-p" "--phases" "Which phases to execute in which order" :default "simultaneous"]
           ["--index" "elasticsearch index name" :default "en-wikipedia"]
           ["--batch" "Batch size for compute operations. Bigger batch requires more heap" :default "256"])
        ]
    (when (or (empty? args) (:help opts))
      (println "Listening for input on stdin (try bzip2 -dcf en-wiki-dump.bz2 | java -jar wikiparse.jar)"))
    [opts (first args)]))

(defn new-phase-stats
  [name]
  {:name name
   :start (System/currentTimeMillis)
   :processed (AtomicLong.)})

(def phase-stats (atom {}))

(defn print-phase-stats
  [{:keys [start processed name] :as stats}]
  (let [total (.get processed)
        now (System/currentTimeMillis)
        elapsed-secs (/ (- now start) 1000.0)
        rate  (/ total elapsed-secs)]
    (println (format "Phase '%s' @ %s in %2f secs. (%2f p/s)"
                     name
                     (.get processed)
                     elapsed-secs
                     rate))))

(defn make-callback
  [{:keys [start processed]  :as stats}]
  (fn [bulk-lines]
    (let [processed-items (/ (count bulk-lines) 2)]
      (.addAndGet processed processed-items)
      (print-phase-stats stats)
      )))


(defn -main
  [& args]
  (let [[opts path] (parse-cmdline args)]
    (with-connection (:es opts) conn
                     (ensure-index conn (:index opts))
                     (doseq [phase (map keyword (string/split (:phases opts) #","))]
                       (let [stats (swap! phase-stats (fn [_] (new-phase-stats phase)))
                             batch-size (Integer/parseInt (:batch opts))
                             callback (make-callback stats)
                             runner (fn [rdr]
                                      (println "Starting phase:" phase)
                                      (println "Batch size:" (:batch opts))
                                      (dorun
                                        (index-dump rdr conn callback phase (:index opts) batch-size)))]
                         (if path
                           (with-open [rdr (bz2-reader path)] (runner rdr))
                           (runner *in*))
                         (print-phase-stats stats)
                         (println "Completed phase:" phase)
                         ))
                     ))
  (System/exit 0))