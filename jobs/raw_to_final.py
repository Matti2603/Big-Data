"""Raw -> Final: Karten aus den API-Seiten lösen, bereinigen, deduplizieren, als Parquet speichern.

Aufruf (aus dem DAG): spark-submit jobs/raw_to_final.py --raw <hdfs-dir> --final <hdfs-dir>
"""

import argparse

from pyspark.sql import Column, functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

from jobs.common import register_final_table, spark_session

# Nur die benötigten Felder; alles andere (z. B. legalities, rulings) wird beim
# Lesen verworfen. Ein festes Schema spart außerdem die Schema-Erkennung über
# alle Dateien.
FOREIGN = StructType([
    StructField("name", StringType()),
    StructField("text", StringType()),
    StructField("type", StringType()),
    StructField("flavor", StringType()),
    StructField("imageUrl", StringType()),
    StructField("language", StringType()),
    StructField("multiverseid", LongType()),
])
CARD = StructType([
    StructField("id", StringType()),
    StructField("multiverseid", StringType()),
    StructField("name", StringType()),
    StructField("manaCost", StringType()),
    StructField("cmc", DoubleType()),
    StructField("colors", ArrayType(StringType())),
    StructField("colorIdentity", ArrayType(StringType())),
    StructField("type", StringType()),
    StructField("supertypes", ArrayType(StringType())),
    StructField("types", ArrayType(StringType())),
    StructField("subtypes", ArrayType(StringType())),
    StructField("rarity", StringType()),
    StructField("set", StringType()),
    StructField("setName", StringType()),
    StructField("text", StringType()),
    StructField("flavor", StringType()),
    StructField("artist", StringType()),
    StructField("number", StringType()),
    StructField("power", StringType()),
    StructField("toughness", StringType()),
    StructField("loyalty", StringType()),
    StructField("layout", StringType()),
    StructField("imageUrl", StringType()),
    StructField("printings", ArrayType(StringType())),
    StructField("foreignNames", ArrayType(FOREIGN)),
])
PAGE = StructType([StructField("cards", ArrayType(CARD))])


def clean(c: Column) -> Column:
    """Leerraum am Rand entfernen, Zeilenumbrüche vereinheitlichen, leere Texte -> NULL."""
    v = F.trim(F.regexp_replace(c, r"\r\n?", "\n"))
    return F.when(v == "", None).otherwise(v)


def https(c: Column) -> Column:
    # Gatherer liefert die Bilder auch per https; vermeidet Mixed Content im Browser
    return F.regexp_replace(clean(c), r"^http://", "https://")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True)
    p.add_argument("--final", required=True)
    args = p.parse_args()

    spark = spark_session("mtg_raw_to_final")
    target = register_final_table(spark, args.final)

    pages = spark.read.schema(PAGE).option("multiLine", True).json(f"{args.raw}/cards_page_*.json")
    raw = pages.select(F.explode("cards").alias("c")).select("c.*").cache()
    german = F.filter("foreignNames", lambda f: f["language"] == "German").getItem(0)

    cards = raw.select(
        clean(F.col("id")).alias("id"),
        clean(F.col("multiverseid")).cast("bigint").alias("multiverseid"),
        clean(F.col("name")).alias("name"),
        clean(F.col("manaCost")).alias("mana_cost"),
        F.col("cmc"),
        F.col("colors"),
        F.col("colorIdentity").alias("color_identity"),
        clean(F.col("type")).alias("type_line"),
        F.col("supertypes"),
        F.col("types"),
        F.col("subtypes"),
        clean(F.col("rarity")).alias("rarity"),
        F.upper(clean(F.col("set"))).alias("set_code"),
        clean(F.col("setName")).alias("set_name"),
        clean(F.col("text")).alias("text"),
        clean(F.col("flavor")).alias("flavor"),
        clean(F.col("artist")).alias("artist"),
        clean(F.col("number")).alias("number"),
        clean(F.col("power")).alias("power"),
        clean(F.col("toughness")).alias("toughness"),
        clean(F.col("loyalty")).alias("loyalty"),
        clean(F.col("layout")).alias("layout"),
        https(F.col("imageUrl")).alias("image_url"),
        F.col("printings"),
        clean(german["name"]).alias("name_de"),
        clean(german["type"]).alias("type_de"),
        clean(german["text"]).alias("text_de"),
        clean(german["flavor"]).alias("flavor_de"),
        german["multiverseid"].alias("multiverseid_de"),
        https(german["imageUrl"]).alias("image_url_de"),
    )

    # Regeln: ohne ID oder Name nicht verwendbar; die API-ID identifiziert eine
    # Druckversion eindeutig, gleichnamige Karten aus anderen Editionen bleiben erhalten.
    valid = cards.where(F.col("id").isNotNull() & F.col("name").isNotNull())
    final = valid.dropDuplicates(["id"])

    # Spalten und Typen exakt wie in ddl/final_cards.sql
    final = final.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target.schema])
    final.repartition(4).write.mode("overwrite").parquet(args.final)

    n_raw, n_valid = raw.count(), valid.count()
    n_final = spark.read.parquet(args.final).count()
    print(f"Karten in Raw:              {n_raw}")
    print(f"ohne ID oder Name verworfen: {n_raw - n_valid}")
    print(f"Duplikate (gleiche ID):      {n_valid - n_final}")
    print(f"Karten in Final:            {n_final}")
    spark.stop()


if __name__ == "__main__":
    main()
