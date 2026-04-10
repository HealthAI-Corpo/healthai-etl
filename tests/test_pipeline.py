"""Tests unitaires — data_pipeline/pipeline.py

Les 3 fonctions execute_pipeline_* construisent un objet PipelineETL
puis appellent execute_pipeline_etl(). On mocke execute_pipeline_etl
pour tester la configuration sans connexion base de données.
"""

from unittest.mock import patch

from src.data_pipeline.pipeline import (
    execute_pipeline_daily_food,
    execute_pipeline_diet_recommendations_dataset,
    execute_pipeline_exercisedb_hobby,
)
from src.data_pipeline.utils import ExtensionFichier, PipelineETL, TypeDonnees


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_TARGET = "src.data_pipeline.pipeline.execute_pipeline_etl"


def capture_pipeline_arg(pipeline_fn, file_path=None):
    """Appelle pipeline_fn en mockant execute_pipeline_etl et retourne
    l'objet PipelineETL passé en argument."""
    with patch(MOCK_TARGET, return_value=[]) as mock_etl:
        pipeline_fn(file_path) if file_path else pipeline_fn()
        assert mock_etl.call_count == 1
        return mock_etl.call_args[0][0]  # premier argument positionnel


# ---------------------------------------------------------------------------
# execute_pipeline_exercisedb_hobby
# ---------------------------------------------------------------------------


def test_exercisedb_retourne_liste():
    with patch(MOCK_TARGET, return_value=["/clean/exercices.csv"]):
        result = execute_pipeline_exercisedb_hobby()
    assert isinstance(result, list)


def test_exercisedb_pipeline_config():
    pipeline = capture_pipeline_arg(execute_pipeline_exercisedb_hobby)
    assert isinstance(pipeline, PipelineETL)
    assert pipeline.table_nom == "exercice"
    assert pipeline.extension_fichier == ExtensionFichier.JSON


def test_exercisedb_colonnes():
    pipeline = capture_pipeline_arg(execute_pipeline_exercisedb_hobby)
    noms_bdd = [c.colonne_bdd for c in pipeline.colonnes]
    assert "nom" in noms_bdd
    assert "type_exercice" in noms_bdd
    assert "muscles_principaux" in noms_bdd
    assert "muscles_secondaires" in noms_bdd
    assert "equipement" in noms_bdd


def test_exercisedb_ids_uniques():
    pipeline = capture_pipeline_arg(execute_pipeline_exercisedb_hobby)
    ids = [c.id_etl_column_mapping for c in pipeline.colonnes]
    assert len(ids) == len(set(ids)), "Les id_etl_column_mapping doivent être uniques"


def test_exercisedb_override_path_transmis():
    with patch(MOCK_TARGET, return_value=[]) as mock_etl:
        execute_pipeline_exercisedb_hobby("/tmp/custom.json")
    _, kwargs = mock_etl.call_args
    assert kwargs.get("override_path") == "/tmp/custom.json"


# ---------------------------------------------------------------------------
# execute_pipeline_daily_food
# ---------------------------------------------------------------------------


def test_daily_food_retourne_liste():
    with patch(MOCK_TARGET, return_value=[]):
        result = execute_pipeline_daily_food()
    assert isinstance(result, list)


def test_daily_food_pipeline_config():
    pipeline = capture_pipeline_arg(execute_pipeline_daily_food)
    assert isinstance(pipeline, PipelineETL)
    assert pipeline.table_nom == "aliment"
    assert pipeline.extension_fichier == ExtensionFichier.CSV


def test_daily_food_colonnes_nutritionnelles():
    pipeline = capture_pipeline_arg(execute_pipeline_daily_food)
    noms_bdd = [c.colonne_bdd for c in pipeline.colonnes]
    for attendu in ("calories", "proteines", "glucides", "lipides", "fibres"):
        assert attendu in noms_bdd, f"Colonne manquante : {attendu}"


def test_daily_food_types_decimaux():
    pipeline = capture_pipeline_arg(execute_pipeline_daily_food)
    cols_decimal = [c for c in pipeline.colonnes if c.colonne_bdd == "calories"]
    assert len(cols_decimal) == 1
    assert cols_decimal[0].type_donnees == TypeDonnees.DECIMAL


# ---------------------------------------------------------------------------
# execute_pipeline_diet_recommendations_dataset
# ---------------------------------------------------------------------------


def test_diet_retourne_liste():
    with patch(MOCK_TARGET, return_value=[]):
        result = execute_pipeline_diet_recommendations_dataset()
    assert isinstance(result, list)


def test_diet_pipeline_config():
    pipeline = capture_pipeline_arg(execute_pipeline_diet_recommendations_dataset)
    assert isinstance(pipeline, PipelineETL)
    assert pipeline.table_nom == "dataset_recommendations_regime"
    assert pipeline.extension_fichier == ExtensionFichier.CSV


def test_diet_colonnes_presentes():
    pipeline = capture_pipeline_arg(execute_pipeline_diet_recommendations_dataset)
    noms_bdd = [c.colonne_bdd for c in pipeline.colonnes]
    for attendu in (
        "age",
        "poids_kg",
        "taille_cm",
        "sexe",
        "recommendation_regime",
        "niveau_activite_physique",
    ):
        assert attendu in noms_bdd, f"Colonne manquante : {attendu}"


def test_diet_ids_uniques():
    pipeline = capture_pipeline_arg(execute_pipeline_diet_recommendations_dataset)
    ids = [c.id_etl_column_mapping for c in pipeline.colonnes]
    assert len(ids) == len(set(ids)), "Les id_etl_column_mapping doivent être uniques"


def test_diet_18_colonnes():
    pipeline = capture_pipeline_arg(execute_pipeline_diet_recommendations_dataset)
    assert len(pipeline.colonnes) == 18
