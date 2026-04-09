import ast
import json

import numpy as np
import pandas as pd

from src.data_pipeline.utils import (
    DateConstraint,
    ETLColumnMapping,
    NumericConstraint,
    StringConstraint,
    TypeDonnees,
)


def handle_missing_values(
    df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    mappings: list[ETLColumnMapping],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gere les valeurs nulles selon nullable et valeur_defaut."""
    df_clean = df.copy()
    anomalies = anomaly_df.copy()

    for mapping in mappings:
        col = mapping.colonne_bdd
        if col not in df_clean.columns:
            continue

        # --- Étape 1 : standardiser les valeurs "vides" en pd.NA ---
        df_clean[col] = df_clean[col].replace(
            ["", " ", None, "null", "NULL", "None", "nan", "NAN", "na", "NA"], pd.NA
        )

        # --- Étape 2 : vérifier nullable ---
        mask_na = df_clean[col].isna()
        if not mask_na.any():
            # pas de valeur manquante → passer à la colonne suivante
            continue

        if mapping.nullable:
            # nullable = True → laisser pd.NA
            continue

        if mapping.valeur_defaut not in ["", " ", None, "null", "NULL", np.nan]:
            # nullable = False mais valeur par défaut définie → remplacer les NA
            df_clean[col] = df_clean[col].fillna(mapping.valeur_defaut)
        else:
            # nullable = False et pas de valeur par défaut → ce sont des anomalies
            # capturer les lignes en anomalies avant suppression
            rows_with_error = df_clean.loc[mask_na].copy()
            rows_with_error["erreur"] = f"La cellule {col} est null"
            anomalies = pd.concat([anomalies, rows_with_error], ignore_index=True)

            # Supprimer ces lignes du df_clean
            df_clean = df_clean.loc[~mask_na].copy()

    df_clean.reset_index(drop=True, inplace=True)
    anomalies.reset_index(drop=True, inplace=True)

    return df_clean, anomalies


def parse_array(value: object) -> list | None:
    """Transforme une valeur en liste python quand c'est possible."""
    if isinstance(value, list):
        return value

    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except Exception:
            # cas "a;b;c"
            if ";" in value:
                return value.split(";")

    return None


def convert_column_type(
    df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    mappings: list[ETLColumnMapping],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convertit les colonnes vers le type cible et deplace les erreurs en anomalies."""
    df_clean = df.copy()
    anomalies = anomaly_df.copy()

    for mapping in mappings:
        col = mapping.colonne_bdd

        if col not in df_clean.columns:
            continue

        series = df_clean[col]

        # region --- Étape 1 : conversion selon type de toutes les lignes de la colonne ---
        if mapping.type_donnees == TypeDonnees.INT:
            # Conversion simple: numérique -> arrondi 0 décimale -> entier nullable
            converted = pd.to_numeric(series, errors="coerce").round(0).astype("Int64")
        elif mapping.type_donnees == TypeDonnees.DECIMAL:
            converted = pd.to_numeric(series, errors="coerce")
        elif mapping.type_donnees == TypeDonnees.STRING:
            converted = series.astype(str)
        elif mapping.type_donnees == TypeDonnees.ARRAY_DELIMITED_JSON:
            # Convertir les listes en string délimité par ";"
            separator = ";"
            converted = series.map(
                lambda x: (
                    separator.join(map(str, parse_array(x)))
                    if parse_array(x) is not None
                    else pd.NA
                )
            )
        elif mapping.type_donnees == TypeDonnees.ARRAY_JSON:
            converted = series.map(
                lambda x: (
                    json.dumps(parse_array(x)) if parse_array(x) is not None else pd.NA
                )
            )
        elif mapping.type_donnees in [TypeDonnees.DATE, TypeDonnees.TIMESTAMP]:
            converted = pd.to_datetime(series, errors="coerce")
            if mapping.type_donnees == TypeDonnees.DATE:
                converted = converted.dt.date
        elif mapping.type_donnees == TypeDonnees.BOOLEAN:
            converted = series.map(
                lambda x: (
                    True
                    if str(x).lower() in ["true", "1"]
                    else False
                    if str(x).lower() in ["false", "0"]
                    else pd.NA
                )
            )
        else:
            converted = series
        # endregion

        # region --- Étape 2 : gérer les valeurs manquantes selon nullable / valeur par défaut ---
        mask_invalid = converted.isna()

        # Si valeur null ok alors ne rien faire de plus
        if mapping.nullable:
            # On laisse les NaN
            print("On laisse les NaN")
            df_clean[col] = converted
            continue

        if mapping.valeur_defaut not in [None, "", np.nan]:
            # Convertir valeur par défaut dans le bon type
            try:
                if mapping.type_donnees == TypeDonnees.INT:
                    # Même logique pour le défaut: numérique -> arrondi -> int
                    default_value = int(round(float(mapping.valeur_defaut)))
                elif mapping.type_donnees == TypeDonnees.DECIMAL:
                    default_value = pd.to_numeric(mapping.valeur_defaut)
                elif mapping.type_donnees in [
                    TypeDonnees.STRING,
                    TypeDonnees.ARRAY_DELIMITED_JSON,
                    TypeDonnees.ARRAY_JSON,
                ]:
                    default_value = str(mapping.valeur_defaut)
                elif mapping.type_donnees in [TypeDonnees.DATE, TypeDonnees.TIMESTAMP]:
                    default_value = pd.to_datetime(mapping.valeur_defaut)
                    if mapping.type_donnees == TypeDonnees.DATE:
                        default_value = default_value.date()
                elif mapping.type_donnees == TypeDonnees.BOOLEAN:
                    default_value = (
                        True
                        if str(mapping.valeur_defaut).lower() in ["true", "1"]
                        else False
                    )
                else:
                    default_value = mapping.valeur_defaut
            except Exception:
                # Mettre à jour les valeurs valides
                df_clean.loc[~mask_invalid, col] = converted.loc[~mask_invalid]
                # Valeur par défaut invalide : toutes les lignes invalides deviennent anomalies
                rows_with_error = df_clean.loc[mask_invalid].copy()
                rows_with_error["erreur"] = (
                    f"La cellule {col} n'est pas convertible et la valeur par défaut '{mapping.valeur_defaut}' est invalide"
                )
                anomalies = pd.concat([anomalies, rows_with_error], ignore_index=True)
                # On supprime ces lignes
                df_clean = df_clean.loc[~mask_invalid]
                continue

            # Remplacer les NaN par valeur par défaut
            converted[mask_invalid] = default_value
            df_clean[col] = converted
        else:
            # Mettre les valeurs converties (IMPORTANT)
            df_clean[col] = converted

            # Identifier les lignes invalides
            rows_with_error = df_clean.loc[mask_invalid].copy()
            rows_with_error["erreur"] = (
                f"La cellule {col} n'est pas convertible en {mapping.type_donnees.value}"
            )

            # Ajouter aux anomalies
            anomalies = pd.concat([anomalies, rows_with_error], ignore_index=True)

            # Garder uniquement les lignes valides
            df_clean = df_clean.loc[~mask_invalid]
        # endregion

    df_clean.reset_index(drop=True, inplace=True)
    anomalies.reset_index(drop=True, inplace=True)

    return df_clean, anomalies


def _build_numeric_constraint_mask(
    series: pd.Series, constraint: NumericConstraint, index: pd.Index
) -> pd.Series:
    mask_invalid = pd.Series(False, index=index)
    numeric_series = pd.to_numeric(series, errors="coerce")

    if constraint.nb_min is not None:
        mask_invalid = mask_invalid | (numeric_series < constraint.nb_min)

    if constraint.nb_max is not None:
        mask_invalid = mask_invalid | (numeric_series > constraint.nb_max)

    if constraint.nb_decimal is not None:
        # Vérifie le nombre de décimales en analysant la valeur d'origine en string
        def too_many_decimals(value):
            if pd.isna(value):
                return False
            s = str(value).strip()
            if s == "" or s.lower() in ["nan", "none", "null"]:
                return False
            if "." in s:
                return len(s.split(".")[-1]) > constraint.nb_decimal
            return False

        decimal_mask = series.apply(too_many_decimals)
        mask_invalid = mask_invalid | decimal_mask

    return mask_invalid


def _build_string_constraint_mask(
    series: pd.Series, constraint: StringConstraint, index: pd.Index
) -> pd.Series:
    mask_invalid = pd.Series(False, index=index)
    str_len = series.astype(str).str.len()

    if constraint.min_length is not None:
        mask_invalid = mask_invalid | (str_len < constraint.min_length)

    if constraint.max_length is not None:
        mask_invalid = mask_invalid | (str_len > constraint.max_length)

    return mask_invalid


def _build_date_constraint_mask(
    series: pd.Series, constraint: DateConstraint, index: pd.Index
) -> pd.Series:
    mask_invalid = pd.Series(False, index=index)
    dt_series = pd.to_datetime(series, errors="coerce")

    if constraint.date_min is not None:
        mask_invalid = mask_invalid | (dt_series < pd.to_datetime(constraint.date_min))

    if constraint.date_max is not None:
        mask_invalid = mask_invalid | (dt_series > pd.to_datetime(constraint.date_max))

    return mask_invalid


def check_column_constraint(
    df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    mappings: list[ETLColumnMapping],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Verifie les contraintes metier et deplace les lignes invalides en anomalies."""
    df_clean = df.copy()
    anomalies = anomaly_df.copy()

    for mapping in mappings:
        col = mapping.colonne_bdd
        if col not in df_clean.columns:
            continue

        constraint = mapping.constraint
        if constraint is None:
            continue

        # Série sur le DataFrame courant (qui peut changer après suppression de lignes)
        series = df_clean[col]

        # --- Contraintes NUMERIC (INT/DECIMAL) ---
        if isinstance(constraint, NumericConstraint):
            mask_invalid = _build_numeric_constraint_mask(
                series, constraint, df_clean.index
            )

        # --- Contraintes STRING / ARRAY stringifiées ---
        elif isinstance(constraint, StringConstraint):
            mask_invalid = _build_string_constraint_mask(
                series, constraint, df_clean.index
            )

        # --- Contraintes DATE/TIMESTAMP ---
        elif isinstance(constraint, DateConstraint):
            mask_invalid = _build_date_constraint_mask(
                series, constraint, df_clean.index
            )

        # BOOLEAN : pas de contrainte dédiée
        else:
            continue

        if mask_invalid.any():
            rows_with_error = df_clean.loc[mask_invalid].copy()
            rows_with_error["erreur"] = (
                f"La cellule {col} ne respecte pas les contraintes de la colonne"
            )
            anomalies = pd.concat([anomalies, rows_with_error], ignore_index=True)

            # Supprimer les lignes invalides
            df_clean = df_clean.loc[~mask_invalid].copy()

    df_clean.reset_index(drop=True, inplace=True)
    anomalies.reset_index(drop=True, inplace=True)

    return df_clean, anomalies
