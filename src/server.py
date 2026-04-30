import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    UploadFile,
    BackgroundTasks,
    HTTPException,
    Request,
    Depends,
)
from fastapi.middleware.cors import CORSMiddleware
import shutil
from time import time

from src.auth.dependencies import require_auth
from src.data_pipeline.pipeline import (
    execute_pipeline_daily_food,
    execute_pipeline_diet_recommendations_dataset,
    execute_pipeline_exercisedb_hobby,
    execute_pipeline_dataset_historique_seance_exercice,
    execute_pipeline_dataset_historique_seance_exercice_synthetic_data,
    run_all_pipelines,
)
from src.data_pipeline.downloader.api_client import run_downloader
from src.utils.logger import configure_logging, logger

load_dotenv()

# Configuration du logging au démarrage
configure_logging()

BASE_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_RAW_DIR = BASE_DATA_DIR / "raw"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

logger.info("Application démarrée | Dossier de données : {}", DATA_RAW_DIR)

""" Endpoint pour télécharger et traiter un fichier CSV ou JSON via une requête POST.
Le fichier est sauvegardé dans data/raw et un traitement de base est lancé en arrière-plan."""
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware pour logger les requêtes HTTP
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware pour logger les détails des requêtes et réponses HTTP."""
    start_time = time()

    logger.info(
        "Requête HTTP reçue | Méthode : {} | Chemin : {} | IP client : {}",
        request.method,
        request.url.path,
        request.client.host if request.client else "unknown",
    )

    try:
        response = await call_next(request)
        process_time = time() - start_time

        logger.info(
            "Réponse HTTP envoyée | Statut : {} | Durée : {:.3f}s | Chemin : {}",
            response.status_code,
            process_time,
            request.url.path,
        )

        response.headers["X-Process-Time"] = str(process_time)
        return response
    except Exception as e:
        process_time = time() - start_time
        logger.error(
            "Erreur lors du traitement de la requête | Chemin : {} | Durée : {:.3f}s | Erreur : {}",
            request.url.path,
            process_time,
            str(e),
            exc_info=True,
        )
        raise


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/upload/{pipeline_type}", status_code=202)
async def upload_file(
    pipeline_type: str,
    file: UploadFile,
    background_tasks: BackgroundTasks,
    _user: dict = Depends(require_auth),
):
    """Endpoint pour télécharger et traiter un fichier CSV ou JSON."""

    logger.info(
        "Upload de fichier reçu | Type : {} | Nom : {}", pipeline_type, file.filename
    )

    # validation de l'extension
    if not file.filename.endswith((".csv", ".json")):
        logger.warning(
            "Format de fichier non supporté | Nom : {} | Extension : {}",
            file.filename,
            file.filename.split(".")[-1],
        )
        raise HTTPException(status_code=400, detail="Format non supporté.")

    # Dictionnaire des pipelines disponibles
    pipelines = {
        "exercices": execute_pipeline_exercisedb_hobby,
        "aliments": execute_pipeline_daily_food,
        "recommendations": execute_pipeline_diet_recommendations_dataset,
        "historique_seance": execute_pipeline_dataset_historique_seance_exercice,
        "historique_seance_synthetic": execute_pipeline_dataset_historique_seance_exercice_synthetic_data,
    }

    # validation du type de pipeline AVANT la sauvegarde
    if pipeline_type not in pipelines:
        logger.warning("Type de pipeline inconnu | Type : {}", pipeline_type)
        raise HTTPException(status_code=400, detail="Type de pipeline inconnu.")

    # sauvegarde du fichier
    dest = DATA_RAW_DIR / file.filename
    try:
        with dest.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        logger.info("Fichier sauvegardé avec succès | Destination : {}", dest)
    except Exception as e:
        logger.error(
            "Erreur lors de la sauvegarde du fichier | Destination : {} | Erreur : {}",
            dest,
            str(e),
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=f"Erreur d'écriture : {e}")

    # Exécution du pipeline
    background_tasks.add_task(pipelines[pipeline_type], str(dest))
    logger.info(
        "Pipeline '{}' lancé en arrière-plan | Fichier : {}",
        pipeline_type,
        file.filename,
    )

    return {"message": "Fichier reçu, traitement lancé", "file": file.filename}


@app.post("/run/{pipeline_type}", status_code=202)
async def run_pipeline(
    pipeline_type: str,
    background_tasks: BackgroundTasks,
    _user: dict = Depends(require_auth),
):
    """Endpoint pour exécuter une pipeline spécifique sans fichier uploadé."""

    logger.info("Exécution d'un pipeline demandée | Type : {}", pipeline_type)

    if pipeline_type == "exercices":
        background_tasks.add_task(execute_pipeline_exercisedb_hobby)
        logger.info("Pipeline 'exercices' lancé en arrière-plan")
    elif pipeline_type == "aliments":
        background_tasks.add_task(execute_pipeline_daily_food)
        logger.info("Pipeline 'aliments' lancé en arrière-plan")
    elif pipeline_type == "recommendations":
        background_tasks.add_task(execute_pipeline_diet_recommendations_dataset)
        logger.info("Pipeline 'recommendations' lancé en arrière-plan")
    elif pipeline_type == "historique_seance":
        background_tasks.add_task(execute_pipeline_dataset_historique_seance_exercice)
        logger.info("Pipeline 'historique_seance' lancé en arrière-plan")
    elif pipeline_type == "historique_seance_synthetic":
        background_tasks.add_task(
            execute_pipeline_dataset_historique_seance_exercice_synthetic_data
        )
        logger.info("Pipeline 'historique_seance_synthetic' lancé en arrière-plan")
    else:
        logger.warning("Type de pipeline inconnu demandé | Type : {}", pipeline_type)
        raise HTTPException(status_code=400, detail="Type de pipeline inconnu.")

    return {
        "message": f"Exécution du pipeline '{pipeline_type}' lancée en arrière-plan"
    }


@app.post("/run-all", status_code=202)
async def run_all_pipelines_endpoint(
    background_tasks: BackgroundTasks,
    _user: dict = Depends(require_auth),
):
    """Endpoint pour exécuter toutes les pipelines ETL en arrière-plan."""
    logger.info("Exécution complète de toutes les pipelines lancée")
    background_tasks.add_task(run_all_pipelines)
    return {
        "message": "Exécution complète de tous les pipelines lancée en arrière-plan"
    }


@app.post("/run-download", status_code=202)
async def run_download(
    background_tasks: BackgroundTasks,
    _user: dict = Depends(require_auth),
):
    """Endpoint pour exécuter la phase EXTRACT (téléchargement des données)."""
    logger.info("Exécution du téléchargement des données lancée")
    background_tasks.add_task(run_downloader)
    return {"message": "Exécution du téléchargement des données lancée en arrière-plan"}
