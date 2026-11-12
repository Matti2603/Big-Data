#!/usr/bin/env bash
# Wartet, bis alle Dienste mit Healthcheck "healthy" melden (max. 10 Minuten).
set -euo pipefail
cd "$(dirname "$0")/.."

services=(hadoop mongo airflow-db airflow-webserver airflow-scheduler web)
deadline=$((SECONDS + 600))

for s in "${services[@]}"; do
  printf "%-18s " "$s"
  while true; do
    id="$(docker compose ps -q "$s")"
    state="$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null || echo starting)"
    if [ "$state" = healthy ]; then echo "healthy"; break; fi
    if [ $SECONDS -ge $deadline ]; then
      echo "nicht healthy ($state) – Logs: docker compose logs $s" >&2
      exit 1
    fi
    sleep 3
  done
done
