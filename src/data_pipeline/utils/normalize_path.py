import os
from pathlib import Path

BASE_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))


def normalize_path(dossier_emplacement: str, data_root: str = BASE_DATA_DIR) -> str:
    """Normalise un chemin relatif au dossier data."""
    dossier_emplacement = dossier_emplacement.replace("\\", "/").replace("//", "/")
    dossier_emplacement = dossier_emplacement.strip("/")
    full_path = os.path.join(data_root, dossier_emplacement)
    return os.path.normpath(full_path)
