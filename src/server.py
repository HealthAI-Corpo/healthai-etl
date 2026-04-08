import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException

load_dotenv()

BASE_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_RAW_DIR = BASE_DATA_DIR / "raw"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

''' Endpoint pour télécharger et traiter un fichier CSV ou JSON via une requête POST.
Le fichier est sauvegardé dans data/raw et un traitement de base est lancé en arrière-plan.'''
app = FastAPI()
@app.post("/upload", status_code=202)
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    
    if not file.filename.endswith(('.csv', '.json')):
        raise HTTPException(status_code=400, detail="Format non supporté (CSV ou JSON uniquement).")
        dest = DATA_RAW_DIR / file.filename
    try:
        with dest.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur d'écriture : {e}")

    # On lance l'ETL en tâche de fond pour ne pas bloquer l'utilisateur
    background_tasks.add_task(run_global_pipeline, str(dest))

    return {"message": "Fichier reçu, traitement démarré", "filename": file.filename}