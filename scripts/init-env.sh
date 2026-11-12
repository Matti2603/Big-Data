#!/usr/bin/env bash
# Legt .env mit zufälligen Passwörtern an. Eine vorhandene .env bleibt unverändert,
# weil MongoDB und Postgres die Passwörter beim ersten Start fest übernehmen.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  echo ".env existiert bereits"
  exit 0
fi

rand() { openssl rand -hex 16; }
# Fernet-Schlüssel: 32 Byte, URL-sicheres Base64
fernet="$(openssl rand -base64 32 | tr '+/' '-_')"
admin_pw="$(rand)"

umask 077
cat > .env <<EOF
# Erzeugt von scripts/init-env.sh – nicht einchecken
AIRFLOW_ADMIN_PASSWORD=$admin_pw
AIRFLOW_DB_PASSWORD=$(rand)
AIRFLOW_FERNET_KEY=$fernet
AIRFLOW_SECRET_KEY=$(rand)
MONGO_ROOT_PASSWORD=$(rand)
MONGO_ETL_PASSWORD=$(rand)
MONGO_WEB_PASSWORD=$(rand)
EOF

echo ".env angelegt. Airflow-Login: admin / $admin_pw"
