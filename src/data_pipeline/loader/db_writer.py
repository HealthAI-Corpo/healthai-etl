import os
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from src.data_pipeline.database import engine, SessionLocal
from src.data_pipeline.models import EtlLog, StatutEtlEnum
from src.data_pipeline.utils import PipelineETL, normalize_path
from src.utils.logger import logger


def save_dataframe_to_csv(df: pd.DataFrame, folder_path: str, file_name: str) -> str:
    """Enregistre un DataFrame en CSV dans le dossier cible."""
    os.makedirs(folder_path, exist_ok=True)

    if not file_name.endswith(".csv"):
        file_name += ".csv"

    full_path = os.path.join(folder_path, file_name)
    df.to_csv(full_path, index=False)

    if os.path.exists(full_path):
        logger.debug("Fichier CSV vérifié sur le disque | Chemin : {}", full_path)
    else:
        logger.error(
            "Le fichier n'a pas été créé malgré to_csv | Chemin : {}", full_path
        )

    return full_path


def ingest_cleaned_data(file_path: str, pipeline: PipelineETL) -> None:
    """Charge un CSV nettoye dans la table cible du pipeline."""
    df = pd.read_csv(file_path)
    try:
        df.to_sql(name=pipeline.table_nom, con=engine, if_exists="append", index=False)
        logger.info(
            "Ingestion en base de données réussie | Table : {} | Lignes ajoutées : {}",
            pipeline.table_nom,
            len(df),
        )
    except SQLAlchemyError as e:
        logger.error(
            "Erreur SQL lors de l'ingestion en base | Table : {} | Erreur : {}",
            pipeline.table_nom,
            str(e),
        )
    except Exception as e:
        logger.error("Erreur inattendue lors de l'ingestion | Erreur : {}", str(e))


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
        logger.error(
            "Impossible de renommer le fichier source | Fichier : {} | Erreur : {}",
            file_path,
            str(e),
        )
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
        logger.info(
            "EtlLog enregistrée en base | Pipeline : {} | Statut : {}",
            libelle_pipeline,
            statut.value,
        )
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(
            "Impossible d'enregistrer l'EtlLog | Pipeline : {} | Erreur : {}",
            libelle_pipeline,
            str(e),
        )
    finally:
        session.close()


def log_etl_validation_error(
    libelle_pipeline: str,
    fichier_nom: str,
    error_message: str,
    source_path: str | None = None,
) -> None:
    """Enregistre une erreur de validation du pipeline dans la table EtlLog."""
    fichier_nom_final = os.path.basename(source_path) if source_path else fichier_nom

    session = SessionLocal()
    try:
        etl_log = EtlLog(
            libelle_pipeline=libelle_pipeline,
            fichier_nom=fichier_nom_final,
            nb_lignes_total=0,
            nb_lignes_valides=0,
            nb_lignes_anomalies=0,
            statut=StatutEtlEnum.FAILURE,
            message=error_message,
        )
        session.add(etl_log)
        session.commit()
        logger.info(
            "Erreur de validation enregistrée en EtlLog | Pipeline : {} | Message : {}",
            libelle_pipeline,
            error_message,
        )
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(
            "Impossible d'enregistrer l'erreur de validation en EtlLog | Pipeline : {} | Erreur : {}",
            libelle_pipeline,
            str(e),
        )
    finally:
        session.close()


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
        logger.warning(
            "Anomalies détectées et sauvegardées | Pipeline : {} | Nombre d'anomalies : {}",
            pipeline.table_nom,
            len(anomalies),
        )
    else:
        logger.debug("Aucune anomalie détectée | Pipeline : {}", pipeline.table_nom)

    # --- INGESTION EN BASE ---
    ingest_cleaned_data(path, pipeline)

    # --- MARQUAGE DU FICHIER SOURCE ---
    renamed_source = None
    if source_path and rename_source:
        renamed_source = mark_source_file_as_processed(source_path)
        if renamed_source:
            logger.info(
                "Fichier source archivé après traitement | Fichier : {}",
                os.path.basename(renamed_source),
            )

    # Enregistrement de l'exécution du pipeline dans EtlLog
    log_etl_execution(
        libelle_pipeline=pipeline.table_nom,
        fichier_nom=clean_file_name,
        df=df,
        anomalies=anomalies,
        source_path=source_path,
    )

    return path, renamed_source
