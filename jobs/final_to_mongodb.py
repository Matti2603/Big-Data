"""Final -> MongoDB: Daten für die Suche anreichern und in die Staging-Collection schreiben.

Aufruf (aus dem DAG): spark-submit jobs/final_to_mongodb.py --final <hdfs-dir> --collection cards_staging
Bricht ab, wenn die Collection danach nicht genau so viele Dokumente enthält wie der Final-Layer Zeilen.
"""

import argparse
import os
import re

from pyspark.sql import Window, functions as F

from jobs.common import register_final_table, spark_session

COLOR_NAMES = {
    "W": "Weiß", "U": "Blau", "B": "Schwarz", "R": "Rot", "G": "Grün",
    "White": "Weiß", "Blue": "Blau", "Black": "Schwarz", "Red": "Rot", "Green": "Grün",
}
BATCH_SIZE = 1000
MANA_SYMBOL = re.compile(r"\{([^}]+)\}")


def to_document(row: dict) -> dict:
    """Spark-Zeile -> MongoDB-Dokument: _id setzen, leere Felder weglassen, Suchfelder ergänzen."""
    from etl.text import fold

    doc = {k: v for k, v in row.items() if v is not None and v != []}
    doc["_id"] = doc.pop("id")
    if doc.get("mana_cost"):
        doc["mana_symbols"] = MANA_SYMBOL.findall(doc["mana_cost"])
    doc["search"] = {
        "name": fold(" | ".join(filter(None, [row.get("name"), row.get("name_de")]))),
        "text": fold(" | ".join(filter(None, [row.get("text"), row.get("text_de")]))),
        "artist": fold(row.get("artist")),
    }
    return doc


def write_partition(rows, target: str) -> None:
    """Läuft je Partition auf den Executors; jede Partition hat eine eigene Verbindung."""
    from pymongo import MongoClient

    client = MongoClient(os.environ["MONGO_URI"])
    collection = client.get_default_database()[target]
    batch = []
    for row in rows:
        batch.append(to_document(row.asDict(recursive=True)))
        if len(batch) >= BATCH_SIZE:
            collection.insert_many(batch, ordered=False)
            batch = []
    if batch:
        collection.insert_many(batch, ordered=False)
    client.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--final", required=True)
    p.add_argument("--collection", required=True)
    args = p.parse_args()

    from etl import mongo

    spark = spark_session("mtg_final_to_mongodb")
    final = register_final_table(spark, args.final)

    # Anreicherung: deutsche Farbnamen und Anzahl der Druckversionen je Kartenname
    color_map = F.create_map(*[F.lit(x) for kv in COLOR_NAMES.items() for x in kv])
    enriched = (
        final
        .withColumn("color_names", F.transform("colors", lambda c: F.coalesce(color_map[c], c)))
        .withColumn("versions", F.count("*").over(Window.partitionBy("name")))
    )

    client, db = mongo.connect()
    mongo.create_collection(db, args.collection, "cards")
    client.close()

    target = args.collection
    enriched.foreachPartition(lambda rows: write_partition(rows, target))

    client, db = mongo.connect()
    written = db[args.collection].count_documents({})
    client.close()
    expected = final.count()
    print(f"Zeilen im Final-Layer: {expected}, Dokumente in {args.collection}: {written}")
    spark.stop()
    if written != expected:
        raise SystemExit(f"Anzahl stimmt nicht überein ({written} != {expected})")


if __name__ == "__main__":
    main()
