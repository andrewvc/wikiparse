# wikiparse

Imports wikipedia data dump XML into elasticsearch.

## Usage

* Download the pages-articles XML dump, find the link on [this page](http://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema). You want pages-articles.xml.bz2. DO NOT UNCOMPRESS THE BZ2 FILE.
* From the releases page, download the [wikiparse JAR](https://github.com/andrewvc/wikiparse/releases)
* Run the jar on the BZ2 file: `java -jar -Xmx1g wikiparse-0.2.1.jar --es http://localhost:9200 /var/lib/elasticsearch/enwiki-latest-pages-articles.xml.bz2`
* The data will be indexed to an index named `en-wikipedia` (by default).
  This can be changed with `--index` parameter.

## License

Wikisample.bz2 Copyright: http://en.wikipedia.org/wiki/Wikipedia:Copyrights
All code and other files Copyright © 2013 Andrew Cholakian and distributed under the Eclipse Public License, the same as Clojure.
