"""Abruf aller Karten von api.magicthegathering.io in den Raw-Layer (HDFS).

Die API liefert die Karten seitenweise (max. 100 pro Seite). Jede Seite wird
unverändert als eigene JSON-Datei gespeichert. Mehrere Seiten werden parallel
abgerufen (MTG_FETCH_WORKERS, Standard 6), weil fast die gesamte Laufzeit
Wartezeit auf die API ist. Bereits vorhandene Seiten des Tages werden
übersprungen, damit ein Retry nach Rate-Limit oder Netzwerkfehler dort
weitermacht, wo der letzte Versuch aufgehört hat.
"""

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from etl import hdfs

API_URL = os.environ.get("MTG_API_URL", "https://api.magicthegathering.io/v1/cards")
PAGE_SIZE = 100
PAUSE_SECONDS = 0.5
MAX_ATTEMPTS = 8
RETRY_STATUS = {429, 500, 502, 503, 504}
WORKERS = int(os.environ.get("MTG_FETCH_WORKERS", "6"))
USER_AGENT = "mtg-kartensuche (DHBW Big Data, Prüfungsprojekt)"


def page_file(page: int) -> str:
    return f"cards_page_{page:04d}.json"


def last_page(link_header: str | None) -> int | None:
    """Letzte Seite aus dem Link-Header, z. B. <...?page=937&pageSize=100>; rel="last"."""
    for part in (link_header or "").split(","):
        if 'rel="last"' in part:
            m = re.search(r"[?&]page=(\d+)", part)
            if m:
                return int(m.group(1))
    return None


def get_page(session: requests.Session, page: int, sleep=time.sleep) -> requests.Response:
    """Eine Seite abrufen; bei Rate-Limit und Serverfehlern mit wachsender Pause wiederholen."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = min(300, 5 * 2**attempt)
        try:
            r = session.get(API_URL, params={"page": page, "pageSize": PAGE_SIZE}, timeout=120)
        except requests.RequestException as e:
            print(f"Seite {page}: {type(e).__name__}, neuer Versuch in {wait} s")
            sleep(wait)
            continue
        if r.status_code == 200:
            return r
        if r.status_code in RETRY_STATUS:
            retry_after = r.headers.get("Retry-After", "")
            if retry_after.isdigit():
                wait = int(retry_after)
            print(f"Seite {page}: HTTP {r.status_code}, neuer Versuch in {wait} s")
            sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Seite {page} nach {MAX_ATTEMPTS} Versuchen nicht abrufbar")


def parse_cards(r: requests.Response, page: int) -> list:
    """Prüft, dass die Antwort gültiges JSON mit einer Kartenliste ist."""
    try:
        cards = r.json()["cards"]
    except (ValueError, KeyError) as e:
        raise RuntimeError(f"Seite {page}: Antwort ist kein gültiges Kartenformat") from e
    if not isinstance(cards, list):
        raise RuntimeError(f"Seite {page}: 'cards' ist keine Liste")
    return cards


def fetch_all(raw_dir: str, max_pages: int | None = None, sleep=time.sleep, workers: int = WORKERS) -> dict:
    """Lädt alle (bzw. die ersten max_pages) Seiten nach raw_dir in HDFS.

    Seite 1 wird zuerst allein abgerufen, weil ihre Header die Seitenzahl liefern.
    Die übrigen Seiten laden `workers` Threads parallel; jeder Thread hat eine
    eigene HTTP-Session und pausiert nach jeder Seite. Schlägt eine Seite endgültig
    fehl, werden die noch nicht gestarteten Seiten abgebrochen und der Fehler
    weitergegeben. Was schon in HDFS liegt, überspringt der nächste Versuch.
    """
    local = threading.local()

    def session() -> requests.Session:
        if not hasattr(local, "session"):
            local.session = requests.Session()
            local.session.headers["User-Agent"] = USER_AGENT
        return local.session

    first = get_page(session(), 1, sleep)
    total_pages = last_page(first.headers.get("Link")) or 1
    total_count = int(first.headers.get("Total-Count", 0))
    pages = min(total_pages, max_pages) if max_pages else total_pages

    existing = {s["pathSuffix"] for s in hdfs.list_dir(raw_dir)}
    todo = [page for page in range(1, pages + 1) if page_file(page) not in existing]
    skipped = pages - len(todo)
    print(f"API meldet {total_count} Karten auf {total_pages} Seiten. "
          f"Abzurufen: {len(todo)} von {pages} Seiten, {workers} parallel")

    def load(page: int) -> tuple[int, str | None]:
        r = first if page == 1 else get_page(session(), page, sleep)
        count = len(parse_cards(r, page))
        hdfs.write(f"{raw_dir}/{page_file(page)}", r.content)
        sleep(PAUSE_SECONDS)
        return count, r.headers.get("Ratelimit-Remaining")

    fetched = cards_total = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch") as pool:
        futures = [pool.submit(load, page) for page in todo]
        try:
            for future in as_completed(futures):
                count, remaining = future.result()
                fetched += 1
                cards_total += count
                if fetched % 50 == 0:
                    print(f"{fetched}/{len(todo)} Seiten, Rate-Limit verbleibend: {remaining}")
        except BaseException:
            for future in futures:
                future.cancel()
            raise

    manifest = {
        "source": API_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "api_total_count": total_count,
        "api_total_pages": total_pages,
        "pages": pages,
        "pages_fetched": fetched,
        "pages_skipped": skipped,
        "workers": workers,
    }
    hdfs.write(f"{raw_dir}/_manifest.json", json.dumps(manifest, indent=2).encode())
    print(f"Fertig: {fetched} Seiten neu ({cards_total} Karten), {skipped} bereits vorhanden")
    return manifest
