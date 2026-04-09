"""Tests unitaires — harmonize/cleaners.py"""

import pandas as pd

from src.data_pipeline.harmonize.cleaners import (
    apply_transformations,
    clean_txt,
    column_mapper,
    generate_anomaly_dataframe,
)
from src.data_pipeline.utils import (
    ConditionFailBehavior,
    ETLColumnMapping,
    ETLColumnTransformation,
    StringConstraint,
    TypeDonnees,
    TypeTransformation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_mapping(
    colonne_bdd: str,
    colonne_fichier: str,
    type_donnees: TypeDonnees = TypeDonnees.STRING,
    nullable: bool = True,
    transformations: list | None = None,
) -> ETLColumnMapping:
    return ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd=colonne_bdd,
        colonne_fichier=colonne_fichier,
        in_file=True,
        type_donnees=type_donnees,
        nullable=nullable,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(id_constraint=1, min_length=0, max_length=200),
        transformations=transformations or [],
    )


# ---------------------------------------------------------------------------
# clean_txt
# ---------------------------------------------------------------------------


class TestCleanTxt:
    def test_strips_whitespace(self):
        df = pd.DataFrame({"nom": ["  Alice  ", " Bob"]})
        result = clean_txt(df)
        assert result["nom"].tolist() == ["Alice", "Bob"]

    def test_replaces_none_string_with_nan(self):
        df = pd.DataFrame({"val": ["none", "NULL", "nan", ""]})
        result = clean_txt(df)
        assert result["val"].isna().all()

    def test_preserves_valid_values(self):
        df = pd.DataFrame({"val": ["pomme", "banane", "cerise"]})
        result = clean_txt(df)
        assert result["val"].tolist() == ["pomme", "banane", "cerise"]

    def test_handles_empty_dataframe(self):
        df = pd.DataFrame({"val": pd.Series([], dtype=str)})
        result = clean_txt(df)
        assert result.empty


# ---------------------------------------------------------------------------
# column_mapper
# ---------------------------------------------------------------------------


class TestColumnMapper:
    def test_renames_column(self):
        df = pd.DataFrame({"name": ["push-up", "squat"]})
        mappings = [make_mapping("nom", "name")]
        result = column_mapper(df, mappings)
        assert "nom" in result.columns
        assert "name" not in result.columns

    def test_missing_source_column_gives_none(self):
        df = pd.DataFrame({"other": ["a", "b"]})
        mappings = [make_mapping("nom", "name")]
        result = column_mapper(df, mappings)
        assert result["nom"].isna().all()

    def test_multiple_mappings(self):
        df = pd.DataFrame({"name": ["push-up"], "type": ["strength"]})
        mappings = [
            make_mapping("nom", "name"),
            make_mapping("type_exercice", "type"),
        ]
        result = column_mapper(df, mappings)
        assert list(result.columns) == ["nom", "type_exercice"]


# ---------------------------------------------------------------------------
# generate_anomaly_dataframe
# ---------------------------------------------------------------------------


class TestGenerateAnomalyDataframe:
    def test_has_erreur_column(self):
        mappings = [make_mapping("nom", "name"), make_mapping("type_exercice", "type")]
        result = generate_anomaly_dataframe(mappings)
        assert "erreur" in result.columns

    def test_has_all_bdd_columns(self):
        mappings = [make_mapping("nom", "name"), make_mapping("type_exercice", "type")]
        result = generate_anomaly_dataframe(mappings)
        assert "nom" in result.columns
        assert "type_exercice" in result.columns

    def test_is_empty(self):
        mappings = [make_mapping("nom", "name")]
        result = generate_anomaly_dataframe(mappings)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# apply_transformations
# ---------------------------------------------------------------------------


class TestApplyTransformations:
    def _make_upper_mapping(self) -> ETLColumnMapping:
        transfo = ETLColumnTransformation(
            id_transformation=1,
            id_etl_column_mapping=1,
            ordre=1,
            type_transformation=TypeTransformation.UPPER,
            condition_fail_behavior=ConditionFailBehavior.ERROR,
        )
        return make_mapping("nom", "nom", transformations=[transfo])

    def test_upper_transformation(self):
        df = pd.DataFrame({"nom": ["push-up", "squat"]})
        anomalies = generate_anomaly_dataframe([self._make_upper_mapping()])
        result_df, result_anom = apply_transformations(
            df, anomalies, [self._make_upper_mapping()]
        )
        assert result_df["nom"].tolist() == ["PUSH-UP", "SQUAT"]
        assert len(result_anom) == 0

    def test_lower_transformation(self):
        transfo = ETLColumnTransformation(
            id_transformation=1,
            id_etl_column_mapping=1,
            ordre=1,
            type_transformation=TypeTransformation.LOWER,
            condition_fail_behavior=ConditionFailBehavior.ERROR,
        )
        mapping = make_mapping("nom", "nom", transformations=[transfo])
        df = pd.DataFrame({"nom": ["PUSH-UP", "SQUAT"]})
        anomalies = generate_anomaly_dataframe([mapping])
        result_df, _ = apply_transformations(df, anomalies, [mapping])
        assert result_df["nom"].tolist() == ["push-up", "squat"]

    def test_multiply_transformation(self):
        transfo = ETLColumnTransformation(
            id_transformation=1,
            id_etl_column_mapping=1,
            ordre=1,
            type_transformation=TypeTransformation.MULTIPLY,
            condition_fail_behavior=ConditionFailBehavior.ERROR,
            value_num=2.0,
        )
        mapping = ETLColumnMapping(
            id_etl_column_mapping=1,
            colonne_bdd="poids",
            colonne_fichier="poids",
            in_file=True,
            type_donnees=TypeDonnees.DECIMAL,
            nullable=False,
            valeur_defaut=None,
            unique_constraint=False,
            transformations=[transfo],
        )
        df = pd.DataFrame({"poids": ["10", "20", "30"]})
        anomalies = generate_anomaly_dataframe([mapping])
        result_df, result_anom = apply_transformations(df, anomalies, [mapping])
        assert result_df["poids"].astype(float).tolist() == [20.0, 40.0, 60.0]

    def test_age_to_birthdate(self):
        transfo = ETLColumnTransformation(
            id_transformation=1,
            id_etl_column_mapping=1,
            ordre=1,
            type_transformation=TypeTransformation.AGE_TO_BIRTHDATE,
            condition_fail_behavior=ConditionFailBehavior.ERROR,
        )
        mapping = ETLColumnMapping(
            id_etl_column_mapping=1,
            colonne_bdd="date_naissance",
            colonne_fichier="date_naissance",
            in_file=True,
            type_donnees=TypeDonnees.DATE,
            nullable=False,
            valeur_defaut=None,
            unique_constraint=False,
            transformations=[transfo],
        )
        df = pd.DataFrame({"date_naissance": ["25"]})
        anomalies = generate_anomaly_dataframe([mapping])
        result_df, _ = apply_transformations(df, anomalies, [mapping])
        # La valeur doit être une date au 1er janvier (format timestamp ou date)
        val = str(result_df["date_naissance"].iloc[0])
        assert "-01-01" in val

    def test_nan_rows_become_anomalies_on_error_behavior(self):
        transfo = ETLColumnTransformation(
            id_transformation=1,
            id_etl_column_mapping=1,
            ordre=1,
            type_transformation=TypeTransformation.MULTIPLY,
            condition_fail_behavior=ConditionFailBehavior.ERROR,
            value_num=2.0,
        )
        mapping = ETLColumnMapping(
            id_etl_column_mapping=1,
            colonne_bdd="val",
            colonne_fichier="val",
            in_file=True,
            type_donnees=TypeDonnees.DECIMAL,
            nullable=False,
            valeur_defaut=None,
            unique_constraint=False,
            transformations=[transfo],
        )
        # "abc" ne peut pas être converti en numérique → anomalie
        df = pd.DataFrame({"val": ["10", "abc", "30"]})
        anomalies = generate_anomaly_dataframe([mapping])
        result_df, result_anom = apply_transformations(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 1
