import os
import re
from src.data_pipeline.database import SessionLocal
from src.data_pipeline.models import EtlLog
import pandas as pd

from src.data_pipeline.utils import PipelineETL, normalize_path
from src.utils.logger import logger

FileWithDataFrame = tuple[str, pd.DataFrame]


def build_filename_pattern(nom_fixe: str, nom_variable: str, extension: str) -> str:
    """Construit un motif regex pour retrouver les fichiers d'un pipeline."""
    if not extension.startswith("."):
        extension = "." + extension

    if nom_variable:
        # Le nom variable a une longueur connue : on remplace par autant de "."
        motif = (
            f"^{re.escape(nom_fixe)}{'.' * len(nom_variable)}{re.escape(extension)}$"
        )
    else:
        motif = f"^{re.escape(nom_fixe)}{re.escape(extension)}$"

    return motif


def find_matching_files(dossier: str, motif_regex: str) -> list[str]:
    """Retourne les chemins complets des fichiers qui matchent le motif."""
    matched_files = []
    if os.path.exists(dossier) and os.path.isdir(dossier):
        for f in os.listdir(dossier):
            if re.match(motif_regex, f):
                matched_files.append(os.path.join(dossier, f))  # chemin complet
    else:
        logger.warning(
            "Le dossier n'existe pas ou n'est pas un répertoire | Dossier : {}", dossier
        )
    return matched_files


def read_single_file_with_pandas(file_path: str) -> pd.DataFrame | None:
    """Lit un CSV/JSON et retourne un DataFrame, sinon None en cas d'échec."""
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(
                file_path, header=0, on_bad_lines="skip"
            )  # header obligatoire et saute les lignes incorrectes (nb colonne different du header)
        elif ext == ".json":
            df = pd.read_json(file_path)
        else:
            logger.warning(
                "Extension de fichier non supportée | Fichier : {} | Extension : {}",
                file_path,
                os.path.splitext(file_path)[1],
            )
            return None
        return df
    except Exception as e:
        logger.error(
            "Impossible de lire le fichier | Fichier : {} | Erreur : {}",
            file_path,
            str(e),
        )
        return None


def read_files_with_pandas(file_paths: list[str]) -> list[FileWithDataFrame]:
    """Lit une liste de fichiers et retourne des couples (file_path, dataframe)."""
    files_with_df: list[FileWithDataFrame] = []
    for fpath in file_paths:
        df = read_single_file_with_pandas(fpath)
        if df is not None:
            files_with_df.append((fpath, df))
    return files_with_df


def get_df_matched_files(pipeline: PipelineETL) -> list[FileWithDataFrame]:
    """Extrait les fichiers correspondant au pattern puis lit leur DataFrame."""
    # 1. Normalisation du chemin
    normalized_folder = normalize_path(pipeline.dossier_emplacement)

    # 2. Construction du motif regex
    pattern = build_filename_pattern(
        pipeline.nom_fichier_fixe,
        pipeline.nom_fichier_variable,
        pipeline.extension_fichier.value,  # Enum → string
    )
    
    # --- FILTRAGE VIA LA TABLE ETL_LOG ---
    all_matched_paths = find_matching_files(normalized_folder, pattern)
    files_to_process = []
    
    with SessionLocal() as session:
        for file_path in all_matched_paths:
            file_name = os.path.basename(file_path)
            
            # On vérifie si un log "SUCCESS" existe déjà pour ce nom de fichier
            already_done = session.query(EtlLog).filter(
                EtlLog.fichier_nom == file_name,
                EtlLog.statut == "SUCCESS"
            ).first()

            if not already_done:
                files_to_process.append(file_path)
            else:
                logger.info(f"[SKIP] Le fichier {file_name} a déjà été intégré avec succès.")

    # 3. Recherche des fichiers
    matched_files_path = find_matching_files(normalized_folder, pattern)

    # 4. Ouvre les fichiers en retournant des couples (file_path, dataframe)
    files_with_df = read_files_with_pandas(matched_files_path)

    return files_with_df
