import os

DATA_ROOT = "data"  # TODO: mettre en .env


def normalize_path(dossier_emplacement: str, data_root: str = DATA_ROOT) -> str:
    """Normalise un chemin relatif au dossier data."""
    dossier_emplacement = dossier_emplacement.replace("\\", "/").replace("//", "/")
    dossier_emplacement = dossier_emplacement.strip("/")
    full_path = os.path.join(data_root, dossier_emplacement)
    return os.path.normpath(full_path)
