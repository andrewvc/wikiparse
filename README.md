# wikiparse

Imports wikipedia data dump XML into elasticsearch.



## Usage

* [NOTE] This is the most cross-platform way to run the script. See the section on running it faster below for a 2-4x optimization in terms of load time.
* Download the pages-articles XML dump, find the link on [this page](http://en.wikipedia.org/wiki/Wikipedia:Database_download#XML_schema). You want pages-articles.xml.bz2. DO NOT UNCOMPRESS THE BZ2 FILE.
* From the releases page, download the [wikiparse JAR](https://github.com/andrewvc/wikiparse/releases)
* Run the jar on the BZ2 file: `java -jar -Xmx3g -Xms3g wikiparse-0.2.1.jar --es http://localhost:9200 /var/lib/elasticsearch/enwiki-latest-pages-articles.xml.bz2`
* The data will be indexed to an index named `en-wikipedia` (by default).
  This can be changed with `--index` parameter.

# Running it Faster

The fastest way to run this code is by using the run-fast.sh shell script in this repo. This shells out to your OS's bzip2
 program helping with parallelism (at the expense of having to uncompress the bzip2 file twice. This also makes two passes over the input file optimizing the writes to elasticsearch.
The source code of the `run-fast.sh` script is included below.

```bash
#!/bin/sh
JAR=$1
DUMP=$2
curl -XDELETE http://localhost:9200
bzip2 -dcf $DUMP | java -Xmx3g -Xms3g -jar $JAR -p redirects && bzip2 -dcf $DUMP | java -Xmx3g -Xms3g -jar $JAR -p full
```

## License

Wikisample.bz2 Copyright: http://en.wikipedia.org/wiki/Wikipedia:Copyrights
All code and other files Copyright © 2013 Andrew Cholakian and distributed under the Eclipse Public License, the same as Clojure.
