from etl.text import fold


def test_fold_removes_case_accents_and_extra_spaces():
    assert fold("  Ausgewählter   der Ahnfrau ") == "ausgewahlter der ahnfrau"


def test_fold_german_sharp_s_and_ligatures():
    assert fold("Weiß") == fold("WEISS") == "weiss"
    assert fold("Æther Vial") == "aether vial"


def test_fold_empty_values():
    assert fold(None) == ""
    assert fold("") == ""
