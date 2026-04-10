import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException
import shutil

from src.data_pipeline.pipeline import (
    execute_pipeline_daily_food,
    execute_pipeline_diet_recommendations_dataset,
    execute_pipeline_exercisedb_hobby,
)

load_dotenv()

BASE_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_RAW_DIR = BASE_DATA_DIR / "raw"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

""" Endpoint pour télécharger et traiter un fichier CSV ou JSON via une requête POST.
Le fichier est sauvegardé dans data/raw et un traitement de base est lancé en arrière-plan."""
app = FastAPI()


@app.post("/upload/{pipeline_type}", status_code=202)
async def upload_file(
    pipeline_type: str, file: UploadFile, background_tasks: BackgroundTasks
):
    # validation de l'extension
    if not file.filename.endswith((".csv", ".json")):
        raise HTTPException(status_code=400, detail="Format non supporté.")

    # sauvegarde du fichier
    dest = DATA_RAW_DIR / file.filename
    try:
        with dest.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'écriture : {e}")

    # selection du pipeline exercices ou les autres
    if pipeline_type == "exercices":
        background_tasks.add_task(execute_pipeline_exercisedb_hobby, str(dest))
    elif pipeline_type == "aliments":
        background_tasks.add_task(execute_pipeline_daily_food, str(dest))
    elif pipeline_type == "recommendations":
        background_tasks.add_task(
            execute_pipeline_diet_recommendations_dataset, str(dest)
        )
    else:
        raise HTTPException(status_code=400, detail="Type de pipeline inconnu.")

    return {"message": "Fichier reçu, traitement lancé", "file": file.filename}
