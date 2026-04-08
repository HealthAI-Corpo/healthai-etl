"""Tests unitaires — harmonize/validators.py"""

import pandas as pd
import pytest

from data_pipeline.harmonize.validators import (
    check_column_constraint,
    convert_column_type,
    handle_missing_values,
)
from data_pipeline.utils import (
    DateConstraint,
    ETLColumnMapping,
    NumericConstraint,
    StringConstraint,
    TypeDonnees,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_mapping(
    colonne_bdd: str,
    type_donnees: TypeDonnees = TypeDonnees.STRING,
    nullable: bool = True,
    valeur_defaut: str | None = None,
    constraint=None,
) -> ETLColumnMapping:
    return ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd=colonne_bdd,
        colonne_fichier=colonne_bdd,
        in_file=True,
        type_donnees=type_donnees,
        nullable=nullable,
        valeur_defaut=valeur_defaut,
        unique_constraint=False,
        constraint=constraint,
        transformations=[],
    )


def empty_anomalies(mappings: list) -> pd.DataFrame:
    cols = [m.colonne_bdd for m in mappings] + ["erreur"]
    return pd.DataFrame(columns=cols)


# ---------------------------------------------------------------------------
# handle_missing_values
# ---------------------------------------------------------------------------


class TestHandleMissingValues:
    def test_nullable_leaves_nan(self):
        df = pd.DataFrame({"nom": [None, "Alice"]})
        mapping = make_mapping("nom", nullable=True)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = handle_missing_values(df, anomalies, [mapping])
        assert result_df["nom"].isna().sum() == 1
        assert len(result_anom) == 0

    def test_non_nullable_without_default_creates_anomaly(self):
        df = pd.DataFrame({"nom": [None, "Alice"]})
        mapping = make_mapping("nom", nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = handle_missing_values(df, anomalies, [mapping])
        assert len(result_df) == 1
        assert result_df["nom"].iloc[0] == "Alice"
        assert len(result_anom) == 1

    def test_non_nullable_with_default_fills_value(self):
        df = pd.DataFrame({"nom": [None, "Alice"]})
        mapping = make_mapping("nom", nullable=False, valeur_defaut="inconnu")
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = handle_missing_values(df, anomalies, [mapping])
        assert result_df["nom"].iloc[0] == "inconnu"
        assert len(result_anom) == 0

    def test_no_missing_values_unchanged(self):
        df = pd.DataFrame({"nom": ["Alice", "Bob"]})
        mapping = make_mapping("nom", nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = handle_missing_values(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 0


# ---------------------------------------------------------------------------
# convert_column_type
# ---------------------------------------------------------------------------


class TestConvertColumnType:
    def test_converts_to_int(self):
        df = pd.DataFrame({"age": ["25", "30", "40"]})
        mapping = make_mapping("age", TypeDonnees.INT, nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = convert_column_type(df, anomalies, [mapping])
        assert result_df["age"].tolist() == [25, 30, 40]
        assert len(result_anom) == 0

    def test_invalid_int_becomes_anomaly(self):
        df = pd.DataFrame({"age": ["25", "abc", "40"]})
        mapping = make_mapping("age", TypeDonnees.INT, nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = convert_column_type(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 1

    def test_converts_to_decimal(self):
        df = pd.DataFrame({"poids": ["70.5", "80.0"]})
        mapping = make_mapping("poids", TypeDonnees.DECIMAL, nullable=True)
        anomalies = empty_anomalies([mapping])
        result_df, _ = convert_column_type(df, anomalies, [mapping])
        assert result_df["poids"].tolist() == [70.5, 80.0]

    def test_converts_to_boolean(self):
        df = pd.DataFrame({"actif": ["true", "false", "1", "0"]})
        mapping = make_mapping("actif", TypeDonnees.BOOLEAN, nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, _ = convert_column_type(df, anomalies, [mapping])
        assert result_df["actif"].tolist() == [True, False, True, False]

    def test_converts_to_date(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2023-06-15"]})
        mapping = make_mapping("date", TypeDonnees.DATE, nullable=False)
        anomalies = empty_anomalies([mapping])
        result_df, _ = convert_column_type(df, anomalies, [mapping])
        assert str(result_df["date"].iloc[0]) == "2024-01-01"

    def test_array_delimited_json(self):
        df = pd.DataFrame({"muscles": ['["chest", "triceps"]']})
        mapping = make_mapping(
            "muscles", TypeDonnees.ARRAY_DELIMITED_JSON, nullable=False
        )
        anomalies = empty_anomalies([mapping])
        result_df, _ = convert_column_type(df, anomalies, [mapping])
        assert "chest" in result_df["muscles"].iloc[0]
        assert "triceps" in result_df["muscles"].iloc[0]


# ---------------------------------------------------------------------------
# check_column_constraint
# ---------------------------------------------------------------------------


class TestCheckColumnConstraint:
    def test_string_max_length_violated(self):
        df = pd.DataFrame({"nom": ["ok", "x" * 201]})
        mapping = make_mapping(
            "nom",
            constraint=StringConstraint(id_constraint=1, min_length=0, max_length=200),
        )
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 1
        assert len(result_anom) == 1

    def test_string_min_length_violated(self):
        df = pd.DataFrame({"nom": ["ab", "x"]})
        mapping = make_mapping(
            "nom",
            constraint=StringConstraint(id_constraint=1, min_length=2, max_length=200),
        )
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 1
        assert len(result_anom) == 1

    def test_numeric_constraint_min(self):
        df = pd.DataFrame({"age": ["25", "-5", "30"]})
        mapping = make_mapping(
            "age",
            TypeDonnees.INT,
            constraint=NumericConstraint(id_constraint=1, nb_min=0, nb_max=150),
        )
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 1

    def test_numeric_constraint_max(self):
        df = pd.DataFrame({"age": ["25", "200", "30"]})
        mapping = make_mapping(
            "age",
            TypeDonnees.INT,
            constraint=NumericConstraint(id_constraint=1, nb_min=0, nb_max=150),
        )
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 1

    def test_no_constraint_passes_all(self):
        df = pd.DataFrame({"nom": ["a", "b", "c"]})
        mapping = make_mapping("nom", constraint=None)
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 3
        assert len(result_anom) == 0

    def test_date_constraint(self):
        from datetime import datetime

        df = pd.DataFrame({"date": ["2020-01-01", "1800-01-01", "2023-06-15"]})
        mapping = make_mapping(
            "date",
            TypeDonnees.DATE,
            constraint=DateConstraint(
                id_constraint=1,
                date_min=datetime(1900, 1, 1),
                date_max=datetime(2100, 1, 1),
            ),
        )
        anomalies = empty_anomalies([mapping])
        result_df, result_anom = check_column_constraint(df, anomalies, [mapping])
        assert len(result_df) == 2
        assert len(result_anom) == 1
