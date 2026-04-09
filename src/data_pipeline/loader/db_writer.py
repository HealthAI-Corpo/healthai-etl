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


def loader_pipeline(
    df: pd.DataFrame, anomalies: pd.DataFrame, pipeline: PipelineETL
) -> str:
    """Sauvegarde les fichiers clean/anomalies puis insere les donnees en base."""
    normalized_folder = normalize_path(pipeline.dossier_clean_emplacement)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
    clean_file_name = (
        pipeline.nom_fichier_fixe + pipeline.nom_fichier_variable + timestamp
    )
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

    return path
