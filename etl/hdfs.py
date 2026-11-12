"""Zugriff auf HDFS über WebHDFS (REST), ohne Hadoop-Client im Airflow-Container."""

import os

import requests

NAMENODE = os.environ.get("HDFS_NAMENODE_HTTP", "http://hadoop:9870")
USER = os.environ.get("HADOOP_USER_NAME", "hadoop")
TIMEOUT = 120


def _request(method: str, path: str, op: str, **params) -> requests.Response:
    return requests.request(
        method,
        f"{NAMENODE}/webhdfs/v1{path}",
        params={"op": op, "user.name": USER, **params},
        timeout=TIMEOUT,
        allow_redirects=False,
    )


def mkdirs(paths: list[str]) -> None:
    for path in paths:
        _request("PUT", path, "MKDIRS").raise_for_status()
        print(f"HDFS-Verzeichnis vorhanden: {path}")


def list_dir(path: str) -> list[dict]:
    """Einträge eines Verzeichnisses; leere Liste, wenn es nicht existiert."""
    r = _request("GET", path, "LISTSTATUS")
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]


def write(path: str, data: bytes) -> None:
    """Schreibt eine Datei atomar: erst unter temporärem Namen, dann umbenennen.

    So bleibt nach einem Abbruch keine halbe Datei unter dem endgültigen Namen
    liegen, und ein erneuter Lauf kann vorhandene Dateien als vollständig ansehen.
    """
    tmp = f"{path}._tmp"
    # WebHDFS: Der NameNode antwortet mit einer Weiterleitung auf den DataNode
    r = _request("PUT", tmp, "CREATE", overwrite="true")
    if r.status_code != 307:
        r.raise_for_status()
        raise RuntimeError(f"Unerwartete Antwort von WebHDFS ({r.status_code}) für {tmp}")
    requests.put(r.headers["Location"], data=data, timeout=TIMEOUT).raise_for_status()

    delete(path, recursive=False)
    r = _request("PUT", tmp, "RENAME", destination=path)
    r.raise_for_status()
    if not r.json().get("boolean"):
        raise RuntimeError(f"Umbenennen von {tmp} nach {path} fehlgeschlagen")


def delete(path: str, recursive: bool = True) -> None:
    _request("DELETE", path, "DELETE", recursive=str(recursive).lower()).raise_for_status()


def cleanup_raw(raw_root: str, keep: int) -> list[str]:
    """Löscht alte Tagesverzeichnisse im Raw-Layer, die neuesten `keep` bleiben."""
    days = sorted(s["pathSuffix"] for s in list_dir(raw_root) if s["type"] == "DIRECTORY")
    removed = days[:-keep] if keep > 0 else days
    for day in removed:
        delete(f"{raw_root}/{day}")
        print(f"Rohdaten gelöscht: {raw_root}/{day}")
    print(f"Behalten: {days[len(removed):]}")
    return removed
