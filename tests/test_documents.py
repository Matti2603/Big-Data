from jobs.final_to_mongodb import to_document


def test_document_has_id_search_fields_and_no_empty_values():
    row = {
        "id": "b7c1", "name": "Manta Riders", "name_de": "Mantareiter", "mana_cost": "{U}",
        "text": "Flying", "text_de": None, "artist": "Kaja Foglio", "colors": [], "flavor": None,
    }
    doc = to_document(row)

    assert doc["_id"] == "b7c1" and "id" not in doc
    assert "colors" not in doc and "flavor" not in doc and "text_de" not in doc
    assert doc["mana_symbols"] == ["U"]
    assert doc["search"] == {"name": "manta riders | mantareiter", "text": "flying", "artist": "kaja foglio"}


def test_hybrid_mana_symbols():
    assert to_document({"id": "x", "name": "y", "mana_cost": "{2}{W/U}{W/U}"})["mana_symbols"] == ["2", "W/U", "W/U"]
