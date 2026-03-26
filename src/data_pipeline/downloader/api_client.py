import os
import requests
import kagglehub
import json
from dotenv import load_dotenv

load_dotenv()

DATA_RAW_DIR = "data/raw"
os.makedirs(DATA_RAW_DIR, exist_ok=True)


def download_from_kaggle(dataset_handle: str):
    """Télécharge un dataset Kaggle avec gestion d'erreurs et de cache."""
    dataset_name = dataset_handle.split("/")[-1]

    # Cache
    existing_files = os.listdir(DATA_RAW_DIR)
    if any(dataset_name.split("-")[0] in f for f in existing_files):
        print(f"[SKIP] {dataset_handle} est déjà présent.")
        return

    print(f"[KAGGLE] Tentative de récupération : {dataset_handle}...")

    try:
        # Téléchargement via kagglehub
        path = kagglehub.dataset_download(dataset_handle)

        # Envoie des fichiers vers data/raw
        for file in os.listdir(path):
            src = os.path.join(path, file)
            dest = os.path.join(DATA_RAW_DIR, file)
            if os.path.isfile(src):
                os.replace(src, dest)
                print(f"Fichier déplacé : {dest}")

    except Exception as e:
        print(f"[ERREUR KAGGLE] Impossible de récupérer {dataset_handle} : {e}")


def fetch_exercisedb_data():
    """Récupère les exercices via l'API ExerciseDB avec gestion d'erreurs."""
    output_path = os.path.join(DATA_RAW_DIR, "exercisedb_hobby.json")

    if os.path.exists(output_path):
        print("[SKIP] API ExerciseDB : Cache local trouvé.")
        return

    api_key = os.getenv("EXERCISE_DB_API_KEY")
    if not api_key:
        print("[ERREUR API] Clé EXERCISE_DB_API_KEY manquante dans le .env")
        return

    base_url = "https://edb-with-videos-and-images-by-ascendapi.p.rapidapi.com/api/v1/exercises"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "edb-with-videos-and-images-by-ascendapi.p.rapidapi.com",
    }

    print("[API] Appel de ExerciseDB ...")

    try:
        # On utilise le timeout pour éviter que le script ne bloque indéfiniment
        response = requests.get(
            base_url, headers=headers, params={"limit": 200}, timeout=30
        )

        # Verification du code HTTP si différent de 200-99 -> except
        response.raise_for_status()

        data = response.json()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"[API] {len(data)} exercices sauvegardés dans {output_path}")

    except requests.exceptions.HTTPError as http_err:
        print(f"[ERREUR HTTP] {http_err}")
    except requests.exceptions.ConnectionError:
        print("[ERREUR CONNEXION] Vérifiez votre accès internet.")
    except Exception as e:
        print(f"[ERREUR INCONNUE API] : {e}")


def run_downloader():
    """Point d'entrée principal de l'extraction."""
    print("--- Démarrage de la phase EXTRACT ---")

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

    print("--- Fin de la phase EXTRACT ---")


if __name__ == "__main__":
    run_downloader()
