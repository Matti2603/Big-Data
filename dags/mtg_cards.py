"""ETL-Workflow MTG-Kartensuche.

api.magicthegathering.io -> HDFS raw (JSON) -> PySpark -> HDFS final (Parquet)
-> PySpark -> MongoDB (cards_staging) -> Prüfung -> MongoDB (cards) -> Webanwendung

Läuft täglich automatisch. Für einen kurzen Testlauf: Trigger mit {"max_pages": 5}.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl import fetch, hdfs, mongo

MTG_ROOT = "/user/hadoop/mtg"
RAW_ROOT = f"{MTG_ROOT}/raw"
RAW_DIR = RAW_ROOT + "/{{ ds }}"
FINAL_DIR = f"{MTG_ROOT}/final/cards"
STAGING = "cards_staging"

SPARK_SUBMIT = "spark-submit --master 'local[*]' --driver-memory 2g --conf spark.ui.enabled=false"

with DAG(
    dag_id="mtg_cards",
    description="MTG-Karten von der API über HDFS und Spark in die Suchdatenbank",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "max_pages": Param(
            None,
            type=["null", "integer"],
            minimum=1,
            description="Nur die ersten N API-Seiten abrufen (Testlauf). Leer = alle Karten.",
        ),
    },
    doc_md=__doc__,
    tags=["mtg", "big-data"],
) as dag:

    create_hdfs_dirs = PythonOperator(
        task_id="create_hdfs_dirs",
        python_callable=hdfs.mkdirs,
        op_args=[[RAW_DIR, f"{MTG_ROOT}/final"]],
    )

    fetch_cards = PythonOperator(
        task_id="fetch_cards",
        python_callable=fetch.fetch_all,
        op_kwargs={"raw_dir": RAW_DIR, "max_pages": "{{ params.max_pages }}"},
        # Rate-Limit der API: lieber länger warten und dort weitermachen, wo abgebrochen wurde
        retries=6,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=3),
    )

    raw_to_final = BashOperator(
        task_id="raw_to_final",
        bash_command=f"{SPARK_SUBMIT} /opt/mtg/jobs/raw_to_final.py "
        f"--raw \"$HDFS_URI{RAW_DIR}\" --final \"$HDFS_URI{FINAL_DIR}\"",
        do_xcom_push=False,
    )

    final_to_mongodb = BashOperator(
        task_id="final_to_mongodb",
        bash_command=f"{SPARK_SUBMIT} /opt/mtg/jobs/final_to_mongodb.py "
        f"--final \"$HDFS_URI{FINAL_DIR}\" --collection {STAGING}",
        do_xcom_push=False,
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=mongo.publish,
        op_kwargs={
            "staging": STAGING,
            "target": "cards",
            "full_run": "{{ params.max_pages is none }}",
            "run_id": "{{ run_id }}",
        },
    )

    cleanup_raw = PythonOperator(
        task_id="cleanup_raw",
        python_callable=hdfs.cleanup_raw,
        op_kwargs={"raw_root": RAW_ROOT, "keep": 3},
    )

    create_hdfs_dirs >> fetch_cards >> raw_to_final >> final_to_mongodb >> publish >> cleanup_raw
