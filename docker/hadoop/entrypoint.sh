#!/usr/bin/env bash
# Startet NameNode und DataNode und legt die Projektverzeichnisse an.
set -euo pipefail

if [ ! -d /data/namenode/current ]; then
  echo "Erster Start: NameNode formatieren"
  hdfs namenode -format -nonInteractive -force
fi

hdfs namenode & nn=$!
hdfs datanode & dn=$!
trap 'kill -TERM "$nn" "$dn" 2>/dev/null; wait' TERM INT

# Verzeichnisse erst nach dem Safe Mode anlegen
(
  until hdfs dfsadmin -safemode wait >/dev/null 2>&1; do sleep 2; done
  hdfs dfs -mkdir -p /user/hadoop/mtg/raw /user/hadoop/mtg/final
  echo "HDFS bereit: /user/hadoop/mtg/{raw,final}"
) &

# Endet einer der beiden Dienste, beendet sich der Container (restart-Policy greift)
wait -n "$nn" "$dn"
exit 1
