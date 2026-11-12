"""Normalisierung von Text für die Suche (wird von ETL und Webanwendung genutzt)."""

import unicodedata

# Ligaturen, die NFKD nicht zerlegt (z. B. "Æther")
_LIGATURES = str.maketrans({"æ": "ae", "œ": "oe"})


def fold(value: str | None) -> str:
    """Kleinschreibung, ohne Akzente, ß -> ss, Leerraum zusammengefasst.

    Dieselbe Funktion bereitet die Suchfelder in MongoDB und die Eingabe im
    Frontend auf, damit "weiß", "Weiss" und "WEISS" dasselbe finden.
    """
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.casefold().translate(_LIGATURES)
    return " ".join(text.split())
