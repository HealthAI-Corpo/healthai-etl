import os
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from src.data_pipeline.database import engine
from src.data_pipeline.utils import PipelineETL, normalize_path


def save_dataframe_to_csv(df: pd.DataFrame, folder_path: str, file_name: str) -> str:
    """Enregistre un DataFrame en CSV dans le dossier cible."""
    os.makedirs(folder_path, exist_ok=True)

    if not file_name.endswith(".csv"):
        file_name += ".csv"

    full_path = os.path.join(folder_path, file_name)
    df.to_csv(full_path, index=False)

    if os.path.exists(full_path):
        print(f"Fichier vérifié sur le disque : {full_path}")
    else:
        print("ERREUR : Le fichier n'a pas été créé malgré to_csv !")

    return full_path


def ingest_cleaned_data(file_path: str, pipeline: PipelineETL) -> None:
    """Charge un CSV nettoye dans la table cible du pipeline."""
    df = pd.read_csv(file_path)
    try:
        df.to_sql(name=pipeline.table_nom, con=engine, if_exists="append", index=False)
        print(
            f"Ingestion réussie : {len(df)} lignes ajoutées dans la table '{pipeline.table_nom}'."
        )
    except SQLAlchemyError as e:
        print(f"Erreur SQL sur la table '{pipeline.table_nom}':", e)
    except Exception as e:
        print("Erreur inattendue :", e)


def mark_source_file_as_processed(file_path: str) -> str | None:
    """Renomme le fichier source en nomActuel.yyyyMMddHHmm.extension."""
    if not file_path or not os.path.exists(file_path):
        return None

    folder, filename = os.path.split(file_path)
    base_name, extension = os.path.splitext(filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    new_filename = f"{base_name}.{timestamp}{extension}"
    new_path = os.path.join(folder, new_filename)

    try:
        os.replace(file_path, new_path)
        return new_path
    except Exception as e:
        print(f"[ERROR] Impossible de renommer le fichier source '{file_path}': {e}")
        return None


def loader_pipeline(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    pipeline: PipelineETL,
    source_path: str | None = None,
    rename_source=True,
) -> tuple[str, str | None]:
    """Sauvegarde les fichiers clean/anomalies puis insère en base."""
    
    # 1. Gestion dynamique des dossiers via DATA_DIR
    data_root = os.getenv("DATA_DIR", "data")
    
    # Dossier Clean (utilisant ton utilitaire de normalisation)
    normalized_clean_folder = normalize_path(pipeline.dossier_clean_emplacement)
    
    # Dossier Anomalies forcé dans data/anomalies
    normalized_anomaly_folder = os.path.join(os.getcwd(), data_root, "anomalies")

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")

    # --- SAUVEGARDE DU FICHIER CLEAN ---
    clean_file_name = f"{pipeline.table_nom}{timestamp}"
    path = save_dataframe_to_csv(df, normalized_clean_folder, clean_file_name)

    # --- SAUVEGARDE DES ANOMALIES (Seulement si le DataFrame n'est pas vide) ---
    if not anomalies.empty:
        anomaly_file_name = f"{pipeline.table_nom}_anomalies{timestamp}"
        save_dataframe_to_csv(anomalies, normalized_anomaly_folder, anomaly_file_name)
        print(f"{len(anomalies)} anomalies détectées et sauvegardées dans /anomalies.")
    else:
        print("Aucune anomalie détectée.")

    # --- INGESTION EN BASE ---
    ingest_cleaned_data(path, pipeline)

    # --- MARQUAGE DU FICHIER SOURCE ---
    renamed_source = None
    if source_path and rename_source:
        renamed_source = mark_source_file_as_processed(source_path)
        if renamed_source:
             print(f"Fichier source archivé : {os.path.basename(renamed_source)}")

    return path, renamed_source