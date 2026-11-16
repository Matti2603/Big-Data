# MTG-Kartensuche – lokale Umgebung mit Docker Compose
# Läuft auch mit dem make 3.81 von macOS.

COMPOSE := docker compose
DAG     := mtg_cards
AIRFLOW := $(COMPOSE) exec -T airflow-scheduler airflow

.PHONY: help env up down reset ps logs trigger trigger-test runs test abgabe

help: ## Diese Hilfe anzeigen
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

env: ## .env mit Zufallspasswörtern anlegen (nur wenn noch nicht vorhanden)
	@bash scripts/init-env.sh

up: env ## Images bauen, alles starten und warten, bis alle Dienste healthy sind
	$(COMPOSE) up -d --build
	@bash scripts/wait-healthy.sh
	@echo ""
	@echo "Ready"
	@echo "  Kartensuche: http://localhost:8000"
	@echo "  Airflow:     http://localhost:8080  (admin / Passwort: AIRFLOW_ADMIN_PASSWORD in .env)"
	@echo "  HDFS:        http://localhost:9870"

down: ## Container stoppen (Daten bleiben erhalten)
	$(COMPOSE) down

reset: ## Container und alle Daten löschen (HDFS, MongoDB, Airflow)
	$(COMPOSE) down -v

ps: ## Zustand der Container
	$(COMPOSE) ps

logs: ## Logs aller Dienste verfolgen
	$(COMPOSE) logs -f --tail=100

trigger: ## Vollständigen ETL-Lauf starten (alle Karten, ca. 5 min)
	$(AIRFLOW) dags trigger $(DAG)

trigger-test: ## Kurzen Testlauf starten (5 API-Seiten = 500 Karten)
	$(AIRFLOW) dags trigger $(DAG) -c '{"max_pages": 5}'

runs: ## Letzte Läufe des DAGs anzeigen
	$(AIRFLOW) dags list-runs -d $(DAG) | head -15

test: ## Unit-Tests im Airflow-Image ausführen
	$(COMPOSE) run --rm --no-deps --entrypoint bash -v ./tests:/opt/mtg/tests:ro -v ./web:/opt/mtg/web:ro \
	  airflow-scheduler -c "cd /opt/mtg && python -m pytest -q -p no:cacheprovider tests"

abgabe: ## Abgabe-ZIP aus dem aktuellen Commit erzeugen (abgabe/mtg-kartensuche.zip)
	@mkdir -p abgabe
	git archive --format=zip --prefix=mtg-kartensuche/ -o abgabe/mtg-kartensuche.zip HEAD
	@echo "abgabe/mtg-kartensuche.zip"
