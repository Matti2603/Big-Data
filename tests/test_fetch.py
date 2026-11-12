import json

import pytest

from etl import fetch


class FakeResponse:
    def __init__(self, status=200, cards=None, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self._body = {"cards": cards if cards is not None else []}
        self.content = json.dumps(self._body).encode()

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.pages = []
        self.headers = {}

    def get(self, url, params, timeout):
        self.pages.append(params["page"])
        return self.responses.pop(0)


def test_last_page_from_link_header():
    link = ('<https://api.magicthegathering.io/v1/cards?page=937&pageSize=100>; rel="last", '
            '<https://api.magicthegathering.io/v1/cards?page=2&pageSize=100>; rel="next"')
    assert fetch.last_page(link) == 937
    assert fetch.last_page(None) is None


def test_get_page_retries_on_rate_limit():
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200)])
    waits = []
    assert fetch.get_page(session, 3, sleep=waits.append).status_code == 200
    assert waits == [7]
    assert session.pages == [3, 3]


def test_get_page_gives_up_after_max_attempts():
    session = FakeSession([FakeResponse(503)] * fetch.MAX_ATTEMPTS)
    with pytest.raises(RuntimeError, match="nicht abrufbar"):
        fetch.get_page(session, 1, sleep=lambda s: None)


def test_get_page_does_not_retry_client_errors():
    session = FakeSession([FakeResponse(404)])
    with pytest.raises(RuntimeError, match="404"):
        fetch.get_page(session, 1, sleep=lambda s: None)


def test_fetch_all_skips_pages_already_in_hdfs(monkeypatch):
    headers = {"Link": '<x?page=3&pageSize=100>; rel="last"', "Total-Count": "250"}
    responses = [FakeResponse(cards=[{"id": "a"}], headers=headers), FakeResponse(cards=[{"id": "c"}])]
    session = FakeSession(responses)
    written = {}
    monkeypatch.setattr(fetch.requests, "Session", lambda: session)
    monkeypatch.setattr(fetch.hdfs, "list_dir", lambda path: [{"pathSuffix": "cards_page_0002.json"}])
    monkeypatch.setattr(fetch.hdfs, "write", lambda path, data: written.__setitem__(path, data))

    manifest = fetch.fetch_all("/raw/2026-11-12", sleep=lambda s: None)

    assert session.pages == [1, 3]  # Seite 2 lag schon vor
    assert set(written) == {
        "/raw/2026-11-12/cards_page_0001.json",
        "/raw/2026-11-12/cards_page_0003.json",
        "/raw/2026-11-12/_manifest.json",
    }
    assert manifest["pages_fetched"] == 2 and manifest["pages_skipped"] == 1


def test_fetch_all_respects_max_pages(monkeypatch):
    headers = {"Link": '<x?page=937&pageSize=100>; rel="last"', "Total-Count": "93643"}
    session = FakeSession([FakeResponse(headers=headers), FakeResponse()])
    monkeypatch.setattr(fetch.requests, "Session", lambda: session)
    monkeypatch.setattr(fetch.hdfs, "list_dir", lambda path: [])
    monkeypatch.setattr(fetch.hdfs, "write", lambda path, data: None)

    assert fetch.fetch_all("/raw/x", max_pages=2, sleep=lambda s: None)["pages"] == 2
    assert session.pages == [1, 2]


def test_invalid_page_is_rejected():
    r = FakeResponse()
    r._body = {"error": "x"}
    with pytest.raises(RuntimeError, match="kein gültiges Kartenformat"):
        fetch.parse_cards(r, 5)
