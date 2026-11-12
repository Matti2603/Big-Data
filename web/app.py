"""Webanwendung der MTG-Kartensuche: liest ausschließlich aus MongoDB (Benutzer mtg_web, nur Leserechte)."""

import math
import os
import re
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient

from search import PAGE_SIZE, build_pipeline

BASE = os.path.dirname(__file__)
CARD_ID = re.compile(r"^[0-9a-f-]{8,64}$")

client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=5_000)
db = client.get_default_database()

app = FastAPI(title="MTG-Kartensuche", docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/static", StaticFiles(directory=os.path.join(BASE, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE, "templates"))

CSP = (
    "default-src 'self'; img-src 'self' https://gatherer.wizards.com https://gatherer-static.wizards.com; "
    "style-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'"
)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = CSP
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


def data_status() -> dict | None:
    return db.meta.find_one({"_id": "cards"})


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    q: str = Query("", max_length=100),
    field: Literal["all", "name", "text", "artist"] = "all",
    unique: bool = False,
    page: int = Query(1, ge=1, le=1000),
):
    context = {"q": q, "field": field, "unique": unique, "page": page, "status": data_status()}
    if q.strip():
        try:
            result = next(db.cards.aggregate(build_pipeline(q, field, unique, page), allowDiskUse=True))
        except ValueError as e:
            context["error"] = str(e)
        else:
            total = result["total"][0]["n"] if result["total"] else 0
            context.update(cards=result["items"], total=total, pages=max(1, math.ceil(total / PAGE_SIZE)))
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/karte/{card_id}", response_class=HTMLResponse)
def card(request: Request, card_id: str):
    if not CARD_ID.match(card_id):
        raise HTTPException(404)
    doc = db.cards.find_one({"_id": card_id}, {"search": 0})
    if doc is None:
        raise HTTPException(404)
    others = list(db.cards.find(
        {"name": doc["name"], "_id": {"$ne": card_id}},
        {"set_code": 1, "set_name": 1, "rarity": 1, "multiverseid": 1},
    ).sort([("set_code", 1)]).limit(100))
    return templates.TemplateResponse(request, "card.html", {"card": doc, "others": others, "status": data_status()})


@app.get("/health")
def health():
    db.command("ping")
    return {"status": "ok"}
