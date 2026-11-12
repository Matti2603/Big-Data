"""Gemeinsame Hilfen der Spark-Jobs."""

from pathlib import Path
from string import Template

from pyspark.sql import DataFrame, SparkSession

DDL_FINAL = Path(__file__).resolve().parent.parent / "ddl" / "final_cards.sql"


def spark_session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def register_final_table(spark: SparkSession, location: str) -> DataFrame:
    """Registriert final_cards aus ddl/final_cards.sql auf dem angegebenen HDFS-Pfad."""
    ddl = Template(DDL_FINAL.read_text(encoding="utf-8")).substitute(location=location)
    spark.sql("DROP TABLE IF EXISTS final_cards")
    spark.sql(ddl.strip().rstrip(";"))
    return spark.table("final_cards")
