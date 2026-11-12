# Build-Kontext ist das Projektverzeichnis, damit etl/text.py (Normalisierung
# der Suchbegriffe) aus derselben Datei kommt wie im ETL.
FROM python:3.12-slim

RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 jinja2==3.1.5 pymongo==4.10.1 \
 && useradd --create-home --uid 1001 web

WORKDIR /app
COPY etl/__init__.py etl/text.py etl/
COPY web/ .

USER web
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=10 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
