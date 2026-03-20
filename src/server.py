"""Serveur FastAPI ETL — placeholder M1.

Expose les endpoints utilisés par le dashboard Next.js (via Traefik Forward Auth) :
  POST /upload  — reçoit un fichier CSV/JSON, le sauvegarde dans data/raw/ et lance le pipeline
  GET  /health  — health check (route publique, exclue du Forward Auth)
"""

import logging
from pathlib import Path

from fastapi import FastAPI, UploadFile

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="HealthAI ETL", version="0.1.0")

DATA_RAW = Path("data/raw")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "healthai-etl"}


@app.post("/upload", status_code=202)
async def upload(file: UploadFile) -> dict:
    """Reçoit un fichier, le sauvegarde dans data/raw/ et déclenche le pipeline.

    TODO M1 : appeler run_pipeline() après la sauvegarde.
    """
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    dest = DATA_RAW / file.filename
    content = await file.read()
    dest.write_bytes(content)
    logger.info("Fichier reçu : %s (%d octets)", file.filename, len(content))
    # TODO M1 : from src.run_pipeline import run_pipeline; run_pipeline()
    return {"saved": str(dest), "size": len(content)}
