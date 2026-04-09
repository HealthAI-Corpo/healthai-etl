import os
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from src.data_pipeline.database import engine
from src.data_pipeline.utils import PipelineETL, normalize_path


def save_dataframe_to_csv(df: pd.DataFrame, folder_path: str, file_name: str) -> str:
    """Enregistre un DataFrame en CSV dans le dossier cible."""
    # S'assurer que le dossier existe
    os.makedirs(folder_path, exist_ok=True)

    # Ajouter .csv si pas présent
    if not file_name.endswith(".csv"):
        file_name += ".csv"

    # Construire le chemin complet
    full_path = os.path.join(folder_path, file_name)

    # Sauvegarde
    df.to_csv(full_path, index=False)
    
    if os.path.exists(full_path):
        print(f"Fichier vérifié sur le disque : {full_path}")
    else:
        print(f"ERREUR : Le fichier n'a pas été créé malgré to_csv !")

    return full_path


def ingest_cleaned_data(file_path: str, pipeline: PipelineETL) -> None:
    """Charge un CSV nettoye dans la table cible du pipeline."""
    # 1. Charger le CSV traité
    df = pd.read_csv(file_path)

    # 2. Envoyer dans la BDD
    # 'name' doit correspondre au tablename du modèle (ex: "profil_sante")
    try:
        df.to_sql(name=pipeline.table_nom, con=engine, if_exists="append", index=False)
        print(f"Ingestion réussie : {len(df)} lignes ajoutées.")

    except SQLAlchemyError as e:
        print("Erreur SQL :", e)

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
) -> tuple[str, str | None]:
    """Sauvegarde les fichiers clean/anomalies puis insere les donnees en base."""
    normalized_folder = normalize_path(pipeline.dossier_clean_emplacement)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    clean_file_name = (
        pipeline.nom_fichier_fixe + pipeline.nom_fichier_variable + timestamp
    )
    print(f"📍 CHEMIN ABSOLU CIBLE : {os.path.abspath(normalized_folder)}")
    path = save_dataframe_to_csv(df, normalized_folder, clean_file_name)

    # TEST POUR LES ANOMALIES
    anomaly_file_name = (
        pipeline.nom_fichier_fixe
        + pipeline.nom_fichier_variable
        + "_anomalies"
        + timestamp
    )
    save_dataframe_to_csv(anomalies, normalized_folder, anomaly_file_name)
    # FIN TEST POUR LES ANOMALIES

    ingest_cleaned_data(path, pipeline)

    # Le renommage du fichier source est centralise ici pour garder un flux ETL unique.
    renamed_source = mark_source_file_as_processed(source_path) if source_path else None

    return path, renamed_source
