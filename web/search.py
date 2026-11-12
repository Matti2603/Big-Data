"""Aufbau der MongoDB-Abfragen für die Kartensuche (ohne Datenbankzugriff, damit testbar)."""

import re

from etl.text import fold

PAGE_SIZE = 25
MIN_LENGTH = 2

# Suchfeld im Formular -> normalisierte Felder in MongoDB (siehe ddl/cards.json)
FIELDS = {
    "name": ["search.name"],
    "text": ["search.text"],
    "artist": ["search.artist"],
    "all": ["search.name", "search.text", "search.artist"],
}

# Nur was die Ergebnisliste anzeigt
LIST_FIELDS = {
    "name": 1, "name_de": 1, "multiverseid": 1, "set_code": 1, "set_name": 1,
    "rarity": 1, "artist": 1, "type_line": 1, "mana_symbols": 1, "image_url": 1, "versions": 1,
}


def build_match(query: str, field: str) -> dict:
    """Teilstring-Suche ohne Beachtung von Groß-/Kleinschreibung und Akzenten.

    Die Eingabe wird wie die Suchfelder normalisiert und für den regulären
    Ausdruck maskiert, Sonderzeichen wie .* wirken also nicht als Muster.
    """
    if field not in FIELDS:
        raise ValueError(f"Unbekanntes Suchfeld: {field}")
    term = fold(query)
    if len(term) < MIN_LENGTH:
        raise ValueError(f"Mindestens {MIN_LENGTH} Zeichen eingeben")
    pattern = {"$regex": re.escape(term)}
    return {"$or": [{path: pattern} for path in FIELDS[field]]}


def build_pipeline(query: str, field: str, unique: bool, page: int) -> list[dict]:
    """Aggregation: filtern, sortieren, optional je Kartenname zusammenfassen, Seite ausschneiden."""
    pipeline: list[dict] = [
        {"$match": build_match(query, field)},
        {"$sort": {"name": 1, "set_code": 1, "_id": 1}},
    ]
    if unique:
        pipeline += [
            {"$group": {"_id": "$name", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"name": 1, "_id": 1}},
        ]
    pipeline.append({
        "$facet": {
            "total": [{"$count": "n"}],
            "items": [
                {"$skip": (page - 1) * PAGE_SIZE},
                {"$limit": PAGE_SIZE},
                {"$project": LIST_FIELDS},
            ],
        }
    })
    return pipeline
