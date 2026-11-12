#!/usr/bin/env bash
# Gesund = Safe Mode aus, ein DataNode aktiv, Projektverzeichnis vorhanden.
# Über JMX/WebHDFS statt "hdfs dfsadmin", das bei jedem Aufruf eine JVM startet.
set -euo pipefail
base=http://localhost:9870

curl -sf "$base/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | grep -q '"NumLiveDataNodes" : 1,'
curl -sf "$base/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo" | grep -q '"Safemode" : ""'
curl -sf "$base/webhdfs/v1/user/hadoop/mtg/final?op=GETFILESTATUS&user.name=hadoop" >/dev/null
