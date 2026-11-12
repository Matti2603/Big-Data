import pytest

from web.search import PAGE_SIZE, build_match, build_pipeline


def test_name_search_is_normalized_substring():
    assert build_match("Wolken-MANTA", "name") == {"$or": [{"search.name": {"$regex": "wolken\\-manta"}}]}


def test_regex_special_characters_are_escaped():
    pattern = build_match(".*", "all")["$or"][0]["search.name"]["$regex"]
    assert pattern == "\\.\\*"


def test_all_searches_every_field():
    paths = [next(iter(c)) for c in build_match("manta", "all")["$or"]]
    assert paths == ["search.name", "search.text", "search.artist"]


@pytest.mark.parametrize("query", ["", " ", "a"])
def test_too_short_query_is_rejected(query):
    with pytest.raises(ValueError):
        build_match(query, "name")


def test_unknown_field_is_rejected():
    with pytest.raises(ValueError):
        build_match("manta", "password")


def test_pipeline_pagination_and_grouping():
    plain = build_pipeline("manta", "name", unique=False, page=3)
    items = plain[-1]["$facet"]["items"]
    assert items[0] == {"$skip": 2 * PAGE_SIZE}
    assert not any("$group" in stage for stage in plain)

    grouped = build_pipeline("manta", "name", unique=True, page=1)
    assert any("$group" in stage for stage in grouped)
