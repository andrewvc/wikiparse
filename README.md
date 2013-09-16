# wikiparse

Imports wikipedia data dump XML into elasticsearch.

## Usage

* Download the pages-articles XML dump, find the link on [this page](http://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema). You want pages-articles.xml.bz2. DO NOT UNCOMPRESS THE BZ2 FILE.
* Download the [wikiparse JAR](http://andrewvc-misc.s3.amazonaws.com/wikiparse-0.1.0.jar)
* Run the jar on the BZ2 file: `java -jar -Xmx1g wikiparse-0.1.0.jar /var/lib/elasticsearch/enwiki-latest-pages-articles.xml.bz2 http://localhost:9200`
* The data will be indexed to an index named `en-wikipedia`.

## License

Wikisample.bz2 Copyright: http://en.wikipedia.org/wiki/Wikipedia:Copyrights
All code and other files Copyright © 2013 Andrew Cholakian and distributed under the Eclipse Public License, the same as Clojure.
