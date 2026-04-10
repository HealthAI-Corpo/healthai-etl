import os
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from src.data_pipeline.database import engine, SessionLocal
from src.data_pipeline.models import EtlLog, StatutEtlEnum
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
        print("ERREUR : Le fichier n'a pas été créé malgré to_csv !")

    return full_path


def ingest_cleaned_data(file_path: str, pipeline: PipelineETL) -> None:
    """Charge un CSV nettoye dans la table cible du pipeline."""
    # 1. Charger le CSV traité
    df = pd.read_csv(file_path)

    # 2. Envoyer dans la BDD
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


def log_etl_execution(
    libelle_pipeline: str,
    fichier_nom: str,
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    source_path: str | None = None,
) -> None:
    """Enregistre l'exécution du pipeline dans la table EtlLog."""
    # Calcul des statistiques
    nb_lignes_total = len(df) + len(anomalies)
    nb_lignes_valides = len(df)
    nb_lignes_anomalies = len(anomalies)

    # Calcul du pourcentage de réussite
    pourcentage_reussite = (
        (nb_lignes_valides / nb_lignes_total * 100) if nb_lignes_total > 0 else 0
    )
    message = f"{pourcentage_reussite:.1f}% sont passés"

    # Détermination du statut
    if nb_lignes_anomalies == 0:
        statut = StatutEtlEnum.SUCCESS
    elif nb_lignes_valides == 0:
        statut = StatutEtlEnum.FAILURE
    else:
        statut = StatutEtlEnum.PARTIAL_FAILURE

    fichier_nom_final = os.path.basename(source_path) if source_path else fichier_nom

    session = SessionLocal()
    try:
        etl_log = EtlLog(
            libelle_pipeline=libelle_pipeline,
            fichier_nom=fichier_nom_final,
            nb_lignes_total=nb_lignes_total,
            nb_lignes_valides=nb_lignes_valides,
            nb_lignes_anomalies=nb_lignes_anomalies,
            statut=statut,
            message=message,
        )
        session.add(etl_log)
        session.commit()
        print(f"EtlLog enregistrée : {libelle_pipeline} - Statut: {statut.value}")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"[ERROR] Impossible d'enregistrer l'EtlLog: {e}")
    finally:
        session.close()


def loader_pipeline(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    pipeline: PipelineETL,
    source_path: str | None = None,
    rename_source=True,
) -> tuple[str, str | None]:
    """Sauvegarde les fichiers clean/anomalies (nommés selon la table BDD) puis insère en base."""
    normalized_folder = normalize_path(pipeline.dossier_clean_emplacement)

    timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")

    # --- CHANGEMENT ICI : On utilise pipeline.table_nom ---
    clean_file_name = f"{pipeline.table_nom}{timestamp}"

    path = save_dataframe_to_csv(df, normalized_folder, clean_file_name)

    # TEST POUR LES ANOMALIES (Utilise aussi le nom de la table)
    anomaly_file_name = f"{pipeline.table_nom}_anomalies{timestamp}"
    save_dataframe_to_csv(anomalies, normalized_folder, anomaly_file_name)
    # FIN TEST POUR LES ANOMALIES

    ingest_cleaned_data(path, pipeline)

    # On ne renomme que si rename_source est True
    renamed_source = None
    if source_path and rename_source:
        renamed_source = mark_source_file_as_processed(source_path)

    # Enregistrement de l'exécution du pipeline dans EtlLog
    log_etl_execution(
        libelle_pipeline=pipeline.table_nom,
        fichier_nom=clean_file_name,
        df=df,
        anomalies=anomalies,
        source_path=source_path,
    )

    return path, renamed_source
