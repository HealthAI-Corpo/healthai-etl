import os
import requests
import kagglehub
import json
import shutil
import time
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


def fetch_exercisedb_data_rapid_api():
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

    logger.info("Appel de l'API ExerciseDB rapid api en cours (pagination)")

    max_pages = int(os.getenv("EXERCISE_DB_MAX_PAGES", "120"))
    all_rows: list[dict] = []
    after_cursor: str | None = None
    used_cursors: list[str] = []

    try:
        for page_index in range(max_pages):
            params: dict[str, str | int] = {"limit": 100}
            if after_cursor:
                params["after"] = after_cursor

            prepared_request = requests.Request(
                method="GET", url=base_url, params=params
            ).prepare()
            logger.debug(
                "ExerciseDB request | URL : {}",
                prepared_request.url,
            )

            # On utilise le timeout pour éviter que le script ne bloque indéfiniment
            response = requests.get(base_url, headers=headers, params=params, timeout=30)

            # Verification du code HTTP si différent de 200-99 -> except
            response.raise_for_status()

            payload = response.json() or {}
            page_data = payload.get("data")
            meta = payload.get("meta") or {}

            if not isinstance(page_data, list):
                logger.warning(
                    "Format inattendu de la réponse ExerciseDB (data non-liste), arrêt de la récupération de ExerciseDB."
                )
                break

            if not page_data:
                if page_index == 0:
                    logger.warning(
                        "Réponse API ExerciseDB vide, aucune donnée à enregistrer"
                    )
                    return
                logger.info(
                    "Page ExerciseDB vide rencontrée, arrêt pagination | Page : {}",
                    page_index + 1,
                )
                break

            all_rows.extend(page_data)

            has_next_page = bool(meta.get("hasNextPage"))
            next_cursor = meta.get("nextCursor")

            logger.info(
                "Page ExerciseDB récupérée | Page : {} | Lignes : {} | Total cumulé : {}",
                page_index + 1,
                len(page_data),
                len(all_rows),
            )

            if not has_next_page:
                break

            if not next_cursor:
                logger.warning(
                    "ExerciseDB - hasNextPage=true mais nextCursor absent, arrêt pagination."
                )
                break

            if next_cursor in used_cursors:
                logger.warning(
                    "ExerciseDB - Cursor déjà utilisé (boucle détectée), arrêt pagination | Cursor : {}",
                    next_cursor,
                )
                break

            used_cursors.append(next_cursor)
            after_cursor = next_cursor

        if not all_rows:
            logger.warning("Aucune donnée ExerciseDB à enregistrer")
            return

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, indent=4, ensure_ascii=False)

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

def fetch_exercisedb_data():
    """Récupère les exercices via l'API ExerciseDB avec gestion d'erreurs."""
    output_path = os.path.join(DATA_RAW_DIR, "exercisedb_hobby_v1.json")

    # Cache: on skip si un fichier de type exercisedb_hobby_v1.*.json existe deja.
    existing_files = os.listdir(DATA_RAW_DIR)
    has_versioned_cache = any(
        filename.startswith("exercisedb_hobby_v1.") and filename.endswith(".json")
        for filename in existing_files
    )

    if has_versioned_cache:
        logger.info("Cache local trouvé pour ExerciseDB V1, téléchargement ignoré")
        return

    base_url = "https://oss.exercisedb.dev/api/v1/exercises"

    logger.info("Appel de l'API ExerciseDB_V1 en cours (pagination)")

    max_pages = int(os.getenv("EXERCISE_DB_MAX_PAGES", "1000"))
    all_rows: list[dict] = []
    after_cursor: str | None = None
    used_cursors: list[str] = []

    try:
        for page_index in range(max_pages):
            params: dict[str, str | int] = {"limit": 100}
            if after_cursor:
                params["after"] = after_cursor

            prepared_request = requests.Request(
                method="GET", url=base_url, params=params
            ).prepare()
            logger.debug(
                "ExerciseDB_V1 request | URL : {}",
                prepared_request.url,
            )

            # On utilise le timeout pour éviter que le script ne bloque indéfiniment.
            # Retry simple en cas de 429 (2 retries max, attente 5s).
            response: requests.Response | None = None
            for attempt_index in range(3):
                response = requests.get(base_url, params=params, timeout=30)

                if response.status_code == 429 and attempt_index < 2:
                    logger.warning(
                        "ExerciseDB_V1 - 429 Too Many Requests, attente 10s puis retry | Tentative : {}/3",
                        attempt_index + 1,
                    )
                    time.sleep(10)
                    continue

                break

            if response is None:
                raise RuntimeError("Aucune réponse HTTP reçue depuis ExerciseDB_V1")

            # Verification du code HTTP si différent de 200-99 -> except
            response.raise_for_status()

            payload = response.json() or {}
            page_data = payload.get("data")
            meta = payload.get("meta") or {}

            if not isinstance(page_data, list):
                logger.warning(
                    "Format inattendu de la réponse ExerciseDB_V1 (data non-liste), arrêt de la récupération de ExerciseDB_V1."
                )
                break

            if not page_data:
                if page_index == 0:
                    logger.warning(
                        "Réponse API ExerciseDB_V1 vide, aucune donnée à enregistrer"
                    )
                    return
                logger.info(
                    "Page ExerciseDB_V1 vide rencontrée, arrêt pagination | Page : {}",
                    page_index + 1,
                )
                break

            all_rows.extend(page_data)

            has_next_page = bool(meta.get("hasNextPage"))
            next_cursor = meta.get("nextCursor")

            logger.info(
                "Page ExerciseDB_V1 récupérée | Page : {} | Lignes : {} | Total cumulé : {}",
                page_index + 1,
                len(page_data),
                len(all_rows),
            )

            if not has_next_page:
                break

            if not next_cursor:
                logger.warning(
                    "ExerciseDB_V1 - hasNextPage=true mais nextCursor absent, arrêt pagination."
                )
                break

            if next_cursor in used_cursors:
                logger.warning(
                    "ExerciseDB_V1 - Cursor déjà utilisé (boucle détectée), arrêt pagination | Cursor : {}",
                    next_cursor,
                )
                break

            used_cursors.append(next_cursor)
            after_cursor = next_cursor

        if not all_rows:
            logger.warning("Aucune donnée ExerciseDB_V1 à enregistrer")
            return

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, indent=4, ensure_ascii=False)

    except requests.exceptions.HTTPError as http_err:
        logger.error(
            "Erreur HTTP lors de l'appel API ExerciseDB_V1 | Erreur : {}", str(http_err)
        )
    except requests.exceptions.ConnectionError:
        logger.error(
            "Erreur de connexion lors de l'appel API ExerciseDB_V1, vérifiez votre accès internet"
        )
    except Exception as e:
        logger.error(
            "Erreur inconnue lors de l'appel API ExerciseDB_V1 | Erreur : {}", str(e)
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

    # On finit par les APIs d'exercice
    fetch_exercisedb_data()
    fetch_exercisedb_data_rapid_api()

    logger.info("Fin de la phase EXTRACT")


if __name__ == "__main__":
    run_downloader()
