import os
import requests
import kagglehub
import json
import shutil
from pathlib import Path
from dotenv import load_dotenv

from src.utils.logger import logger

load_dotenv()

BASE_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_RAW_DIR = BASE_DATA_DIR / "raw"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_from_kaggle(dataset_handle: str):
    """Télécharge un dataset Kaggle avec gestion d'erreurs et de cache."""
    dataset_name = dataset_handle.split("/")[-1]
    existing_files = os.listdir(DATA_RAW_DIR)

    if any(dataset_name.split(".")[0] in f for f in existing_files):
        logger.info(
            "Dataset déjà présent, téléchargement ignoré | Dataset : {}", dataset_handle
        )
        return

    logger.info("Tentative de récupération du dataset | Dataset : {}", dataset_handle)

    try:
        # Téléchargement via kagglehub (stocké dans /root/.cache/... en Docker)
        path = kagglehub.dataset_download(dataset_handle)

        # Envoie des fichiers vers data/raw
        for file in os.listdir(path):
            src = os.path.join(path, file)
            dest = os.path.join(DATA_RAW_DIR, file)
            if os.path.isfile(src):
                # Utilisation de shutil.move au lieu de os.replace
                # Gère automatiquement le transfert entre différents disques/volumes
                shutil.move(src, dest)
                logger.debug("Fichier déplacé | Destination : {}", dest)

    except Exception as e:
        logger.error(
            "Impossible de récupérer le dataset | Dataset : {} | Erreur : {}",
            dataset_handle,
            str(e),
        )


def fetch_exercisedb_data():
    """Récupère les exercices via l'API ExerciseDB avec gestion d'erreurs."""
    output_path = os.path.join(DATA_RAW_DIR, "exercisedb_hobby.json")

    # Cache: on skip si un fichier de type exercisedb_hobby.*.json existe deja.
    existing_files = os.listdir(DATA_RAW_DIR)
    has_versioned_cache = any(
        filename.startswith("exercisedb_hobby.") and filename.endswith(".json")
        for filename in existing_files
    )

    if has_versioned_cache:
        logger.info("Cache local trouvé pour ExerciseDB, téléchargement ignoré")
        return

    api_key = os.getenv("EXERCISE_DB_API_KEY")
    if not api_key:
        logger.error("Clé EXERCISE_DB_API_KEY manquante dans le fichier .env")
        return

    base_url = "https://edb-with-videos-and-images-by-ascendapi.p.rapidapi.com/api/v1/exercises"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "edb-with-videos-and-images-by-ascendapi.p.rapidapi.com",
    }

    logger.info("Appel de l'API ExerciseDB en cours")

    try:
        # On utilise le timeout pour éviter que le script ne bloque indéfiniment
        response = requests.get(
            base_url, headers=headers, params={"limit": 200}, timeout=30
        )

        # Verification du code HTTP si différent de 200-99 -> except
        response.raise_for_status()

        data = response.json()["data"]

        if not data:
            logger.warning("Réponse API ExerciseDB vide, aucune donnée à enregistrer")
            return

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        logger.info(
            "Exercices sauvegardés depuis l'API ExerciseDB | Nombre : {} | Fichier : {}",
            len(data),
            output_path,
        )

    except requests.exceptions.HTTPError as http_err:
        logger.error(
            "Erreur HTTP lors de l'appel API ExerciseDB | Erreur : {}", str(http_err)
        )
    except requests.exceptions.ConnectionError:
        logger.error(
            "Erreur de connexion lors de l'appel API ExerciseDB, vérifiez votre accès internet"
        )
    except Exception as e:
        logger.error(
            "Erreur inconnue lors de l'appel API ExerciseDB | Erreur : {}", str(e)
        )


def run_downloader():
    """Point d'entrée principal de l'extraction."""
    logger.info("Démarrage de la phase EXTRACT")

    kaggle_datasets = [
        "adilshamim8/daily-food-and-nutrition-dataset",
        "ziya07/diet-recommendations-dataset",
        "valakhorasani/gym-members-exercise-dataset",
        "nadeemajeedch/fitness-tracker-dataset",
    ]

    # On boucle sur Kaggle
    for ds in kaggle_datasets:
        download_from_kaggle(ds)

    # On finit par l'API
    fetch_exercisedb_data()

    logger.info("Fin de la phase EXTRACT")


if __name__ == "__main__":
    run_downloader()
