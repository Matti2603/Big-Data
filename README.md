# MTG-Kartensuche

Durchsuchbare Datenbank aller Magic-the-Gathering-Karten. Prüfungsleistung Big Data, Thema „Create MTG Trading Card Database By API“.

**API → HDFS raw (JSON) → PySpark → HDFS final (Parquet) → PySpark → MongoDB → Web**, gesteuert und täglich automatisch ausgeführt von Airflow.

Ausführliche Beschreibung aller Schritte, Regeln und Datenmodelle: [docs/ETL-Workflow.md](docs/ETL-Workflow.md)

## Voraussetzungen

- Docker mit Compose v2 (Docker Desktop unter macOS/Windows), mindestens 6 GB Arbeitsspeicher für Docker
- `make`, `bash`, `openssl`
- Internetzugang (Images bauen, API abrufen)

Läuft auf amd64 und arm64 (Apple Silicon). Das Hadoop-Image wird dafür aus dem offiziellen Hadoop-Paket selbst gebaut.

## Start

```bash
make up
```

- Legt beim ersten Mal `.env` mit Zufallspasswörtern an.
- Baut die Images und startet alle Dienste.
- Wartet, bis alle Dienste healthy sind.

Danach erreichbar:

| Oberfläche | Adresse |
|---|---|
| Kartensuche | http://localhost:8000 |
| Airflow | http://localhost:8080 (Benutzer `admin`, Passwort `AIRFLOW_ADMIN_PASSWORD` aus `.env`) |
| HDFS | http://localhost:9870 (Utilities → Browse the file system → `/user/hadoop/mtg`) |

Beim ersten Start löst Airflow den DAG `mtg_cards` sofort aus. Der vollständige Abruf aller ca. 94.000 Karten dauert rund 20–30 Minuten. Bis dahin meldet die Suche „Noch keine Daten geladen“.

## Bedienung

| Befehl | Wirkung |
|---|---|
| `make trigger` | vollständigen ETL-Lauf starten |
| `make trigger-test` | kurzen Testlauf mit 5 API-Seiten (500 Karten) starten |
| `make runs` | letzte Läufe anzeigen |
| `make ps` / `make logs` | Zustand bzw. Logs der Container |
| `make test` | Unit-Tests |
| `make down` | Container stoppen, Daten bleiben erhalten |
| `make reset` | Container und alle Daten löschen |
| `make abgabe` | Abgabe-ZIP aus dem aktuellen Commit erzeugen |

Ein Testlauf ersetzt den Datenbestand durch die 500 Testkarten. Danach `make trigger` für den vollständigen Stand.

## Projektstruktur

```
dags/mtg_cards.py           Airflow-DAG (6 Tasks)
etl/fetch.py                API-Abruf in den Raw-Layer
etl/hdfs.py                 HDFS-Zugriff über WebHDFS
etl/mongo.py                Collections anlegen, Staging prüfen und veröffentlichen
etl/text.py                 Normalisierung für die Suche (ETL und Web)
jobs/raw_to_final.py        PySpark: Raw → Final
jobs/final_to_mongodb.py    PySpark: Final → MongoDB
ddl/                        eine Datei je Tabelle (final_cards.sql, cards.json, meta.json)
web/                        Webanwendung (FastAPI, Templates, CSS)
docker/                     Images für Hadoop und Airflow, MongoDB-Benutzer
tests/                      Unit-Tests
```

## Fehlerbehebung

| Problem | Lösung |
|---|---|
| Port 8000, 8080, 9870 oder 27017 belegt | in `.env` z. B. `WEB_PORT=8001`, `AIRFLOW_PORT=8081`, `HDFS_UI_PORT=9871`, `MONGO_PORT=27018` setzen |
| `fetch_cards` steht auf „up_for_retry“ | Rate-Limit der API erreicht. Der Task macht nach 10 Minuten automatisch weiter, bereits geladene Seiten werden übersprungen |
| Spark-Task bricht mit Speicherfehler ab | Docker mehr Arbeitsspeicher geben (Docker Desktop → Settings → Resources) |
| Passwörter in `.env` geändert | `make reset`, da MongoDB und Postgres die Passwörter nur beim ersten Start übernehmen |
