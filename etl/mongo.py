"""Endnutzer-Datenbank (MongoDB): Collections nach DDL anlegen und Daten veröffentlichen."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

DDL_DIR = Path(__file__).resolve().parent.parent / "ddl"


def connect():
    from pymongo import MongoClient

    client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=30_000)
    return client, client.get_default_database()


def load_ddl(name: str) -> dict:
    return json.loads((DDL_DIR / f"{name}.json").read_text(encoding="utf-8"))


def create_collection(db, name: str, ddl: str) -> None:
    """Legt eine leere Collection mit dem Validator aus ddl/<ddl>.json an."""
    spec = load_ddl(ddl)
    db.drop_collection(name)
    db.create_collection(name, validator=spec["validator"], validationLevel="strict")


def create_indexes(db, name: str, ddl: str) -> None:
    for idx in load_ddl(ddl)["indexes"]:
        db[name].create_index([tuple(k) for k in idx["keys"]], name=idx["name"])


def ensure_collection(db, name: str) -> None:
    """Legt eine Collection nur an, wenn sie fehlt (für meta)."""
    if name not in db.list_collection_names():
        create_collection(db, name, name)
        create_indexes(db, name, name)


def publish(staging: str, target: str, full_run: bool, run_id: str, min_ratio: float = 0.9) -> int:
    """Prüft die Staging-Collection und ersetzt damit die produktive Collection.

    renameCollection mit dropTarget tauscht in einem Schritt; die Suche sieht
    also entweder den alten oder den neuen Stand, nie eine halb geladene Collection.
    Dass Staging vollständig ist (Dokumente = Zeilen im Final-Layer), prüft bereits
    final_to_mongodb.py. Hier zusätzlich: nicht leer, und bei einem vollständigen
    Lauf nicht um mehr als 10 % kleiner als der alte Stand (z. B. API liefert
    plötzlich nur einen Teil). Testläufe mit max_pages überspringen diese Prüfung.
    """
    client, db = connect()
    try:
        count = db[staging].count_documents({})
        old = db[target].count_documents({}) if target in db.list_collection_names() else 0
        print(f"{staging}: {count} Dokumente, bisher in {target}: {old}")

        if count == 0:
            raise RuntimeError(f"{staging} ist leer, Veröffentlichung abgebrochen")
        if full_run and old and count < old * min_ratio:
            raise RuntimeError(f"Neuer Stand ({count}) ist mehr als {1 - min_ratio:.0%} kleiner als der alte ({old})")

        create_indexes(db, staging, target)
        db[staging].rename(target, dropTarget=True)

        ensure_collection(db, "meta")
        db.meta.replace_one(
            {"_id": target},
            {
                "count": count,
                "published_at": datetime.now(timezone.utc),
                "run_id": run_id,
                "source": "api.magicthegathering.io",
            },
            upsert=True,
        )
        print(f"Veröffentlicht: {target} mit {count} Dokumenten")
        return count
    finally:
        client.close()
