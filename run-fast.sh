#!/bin/sh
JAR=$1
DUMP=$2
curl -XDELETE http://localhost:9200
bzip2 -dcf $DUMP | java -Xmx3g -Xms3g -jar $JAR -p redirects && bzip2 -dcf $DUMP | java -Xmx3g -Xms3g -jar $JAR -p full