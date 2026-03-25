import os
import requests
import kagglehub
from dotenv import load_dotenv

load_dotenv()

DATA_RAW_DIR = "data/raw"
os.makedirs(DATA_RAW_DIR, exist_ok=True)

def download_from_kaggle(dataset_handle: str):
    """Télécharge un dataset Kaggle et le déplace dans data/raw"""
    print(f"Récupération Kaggle : {dataset_handle}...")
    path = kagglehub.dataset_download(dataset_handle)
    
    for file in os.listdir(path):
        src = os.path.join(path, file)
        dest = os.path.join(DATA_RAW_DIR, file)
        if os.path.isfile(src):
            os.replace(src, dest)
            print(f"Fichier déplacé : {dest}")

def download_github_raw(url: str, filename: str):
    """Télécharge un fichier brut depuis GitHub via l'URL Raw"""
    path = os.path.join(DATA_RAW_DIR, filename)
    token = os.getenv("GITHUB_TOKEN")
    
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    print(f"Téléchargement GitHub : {filename}...")
    
    r = requests.get(url, headers=headers)
    r.raise_for_status() # Génère une erreur si le lien est mort (404)
    
    with open(path, "wb") as f:
        f.write(r.content)
    print(f"Fichier sauvegardé : {path}")

def run_downloader():
    # Datasets Kaggle
    kaggle_datasets = [
        "adilshamim8/daily-food-and-nutrition-dataset",
        "ziya07/diet-recommendations-dataset",
        "valakhorasani/gym-members-exercise-dataset",
        "nadeemajeedch/fitness-tracker-dataset"
    ]
    
    # Téléchargement Kaggle
    for ds in kaggle_datasets:
        try:
            download_from_kaggle(ds)
        except Exception as e:
            print(f"❌ Erreur Kaggle {ds}: {e}")

    # Téléchargement GitHub (ExerciseDB)
    exercisedb_url = "https://raw.githubusercontent.com/Joeyybad/exercisedb-api/main/exercisedb.json"
    
    try:
        download_github_raw(exercisedb_url, "exercisedb.json")
    except Exception as e:
        print(f"❌ Erreur ExerciseDB: {e}")

if __name__ == "__main__":
    run_downloader()