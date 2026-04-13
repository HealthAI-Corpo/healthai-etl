import ast
import json

import numpy as np
import pandas as pd

from src.data_pipeline.utils import (
    ConditionFailBehavior,
    ConditionOperator,
    ETLColumnMapping,
    ETLColumnTransformation,
    ETLTransformationCondition,
    TypeTransformation,
)


def column_mapper(df: pd.DataFrame, mappings: list[ETLColumnMapping]) -> pd.DataFrame:
    """Projette et renomme les colonnes selon le mapping de pipeline."""
    result = pd.DataFrame()

    for mapping in mappings:
        if mapping.in_file and mapping.colonne_fichier in df.columns:
            result[mapping.colonne_bdd] = df[mapping.colonne_fichier]
        else:
            result[mapping.colonne_bdd] = None

    return result.astype(str)


def generate_anomaly_dataframe(columns) -> pd.DataFrame:
    """Construit le DataFrame standard d'anomalies avec les colonnes du fichier original + erreur."""
    anomaly_columns = list(columns) + ["erreur"]

    return pd.DataFrame(columns=anomaly_columns)


def clean_txt(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie les colonnes texte et normalise les valeurs manquantes."""
    df_clean = df.copy()
    object_columns = df_clean.select_dtypes(include="object").columns
    missing_values = {"", " ", "none", "null", "nan", "None", "NULL", "na"}

    for col in object_columns:
        series = df_clean[col].astype(str).str.strip()
        series = series.replace(to_replace=r"^\s*$", value=np.nan, regex=True)
        series = series.mask(series.str.lower().isin(missing_values), np.nan)
        df_clean[col] = series

    return df_clean


def evaluate_single_condition(
    series: pd.Series, condition: ETLTransformationCondition
) -> pd.Series:
    """Evalue une condition élémentaire sur une série déjà convertie."""
    if condition.value_num is not None:
        val = condition.value_num
    elif condition.value_str is not None:
        val = condition.value_str
    elif condition.value_date is not None:
        val = condition.value_date
    else:
        return pd.Series(False, index=series.index)

    if condition.operator == ConditionOperator.GT:
        return series > val
    if condition.operator == ConditionOperator.LT:
        return series < val
    if condition.operator == ConditionOperator.GTE:
        return series >= val
    if condition.operator == ConditionOperator.LTE:
        return series <= val
    if condition.operator == ConditionOperator.EQ:
        return series == val
    if condition.operator == ConditionOperator.NEQ:
        return series != val
    if condition.operator == ConditionOperator.IN:
        return series.isin(val)
    if condition.operator == ConditionOperator.NOT_IN:
        return ~series.isin(val)

    return pd.Series(False, index=series.index)


def evaluate_conditions(
    series: pd.Series, conditions: list[ETLTransformationCondition]
) -> pd.Series:
    """Applique une logique AND intra-groupe, OR inter-groupes."""
    if not conditions:
        return pd.Series(True, index=series.index)

    groups: dict[int, list[ETLTransformationCondition]] = {}
    for cond in conditions:
        groups.setdefault(cond.groupe_code, []).append(cond)

    group_results: list[pd.Series] = []
    for group in groups.values():
        group_mask = pd.Series(True, index=series.index)
        for condition in group:
            group_mask &= evaluate_single_condition(series, condition)
        group_results.append(group_mask)

    final_mask = group_results[0]
    for group_mask in group_results[1:]:
        final_mask |= group_mask

    return final_mask


def add_to_anomalies(
    df_clean: pd.DataFrame,
    df_original: pd.DataFrame,
    anomalies: pd.DataFrame,
    mask: pd.Series,
    message: str,
) -> pd.DataFrame:
    """Ajoute dans le DataFrame anomalies les lignes ciblées par le masque, en utilisant _row_id pour l'alignement."""
    if not mask.any():
        return anomalies

    # Utiliser _row_id pour mapper df_clean vers df_original
    row_ids_to_keep = df_clean.loc[mask, "_row_id"].values
    rows = df_original.loc[df_original["_row_id"].isin(row_ids_to_keep)].copy()
    rows["erreur"] = message
    return pd.concat([anomalies, rows], ignore_index=True)


def handle_condition_failure(
    df_clean: pd.DataFrame,
    df_original: pd.DataFrame,
    anomalies: pd.DataFrame,
    transformation: ETLColumnTransformation,
    col: str,
    active_mask: pd.Series,
    error_mask_global: pd.Series,
    fail_mask: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    """Gère SKIP/STOP/ERROR quand une condition échoue."""
    if not fail_mask.any():
        return active_mask, error_mask_global, anomalies

    if transformation.condition_fail_behavior == ConditionFailBehavior.SKIP:
        # On ne fait rien, la transformation sera appliquée seulement aux lignes valides par le parent
        return active_mask, error_mask_global, anomalies

    if transformation.condition_fail_behavior == ConditionFailBehavior.STOP:
        # Les lignes qui échouent sont retirées du flux actif (stop la transformation pour cette colonne)
        active_mask = active_mask & ~fail_mask
        return active_mask, error_mask_global, anomalies

    if transformation.condition_fail_behavior == ConditionFailBehavior.ERROR:
        # Les lignes en échec deviennent anomalies
        anomalies = add_to_anomalies(
            df_clean,
            df_original,
            anomalies,
            fail_mask,
            f"Condition non respectee col : {col} | transfo : {transformation.ordre}",
        )
        active_mask = active_mask & ~fail_mask
        error_mask_global = error_mask_global | fail_mask

    return active_mask, error_mask_global, anomalies


def apply_transformation(
    series: pd.Series, transformation: ETLColumnTransformation, mask: pd.Series
) -> pd.Series:
    """Applique une transformation sur les lignes sélectionnées."""
    try:
        result = series.copy()
        result = series.copy().astype(object)

        # type numérique
        if transformation.type_transformation == TypeTransformation.MULTIPLY:
            result[mask] = series[mask] * transformation.value_num

        elif transformation.type_transformation == TypeTransformation.DIVIDE:
            result[mask] = series[mask] / transformation.value_num

        elif transformation.type_transformation == TypeTransformation.ADD:
            result[mask] = series[mask] + transformation.value_num

        elif transformation.type_transformation == TypeTransformation.SUBTRACT:
            result[mask] = series[mask] - transformation.value_num

        elif transformation.type_transformation == TypeTransformation.ROUND:
            result[mask] = series[mask].round(transformation.value_int)

        elif transformation.type_transformation == TypeTransformation.CLIP_MAX:
            result[mask] = series[mask].clip(upper=transformation.value_num)

        elif transformation.type_transformation == TypeTransformation.CLIP_MIN:
            result[mask] = series[mask].clip(lower=transformation.value_num)

        elif transformation.type_transformation == TypeTransformation.POWER:
            result[mask] = series[mask] ** transformation.value_num

        elif transformation.type_transformation == TypeTransformation.FLOOR:
            # Arrondi à l'entier inférieur
            result[mask] = np.floor(series[mask])

        elif transformation.type_transformation == TypeTransformation.CEIL:
            # Arrondi à l'entier supérieur
            result[mask] = np.ceil(series[mask])

        elif transformation.type_transformation == TypeTransformation.AGE_TO_BIRTHDATE:
            # Transforme un age en date de naissance fixee au 1er janvier (annee courante - age).
            ages_floor = np.floor(series[mask]).astype("Int64")
            birth_years = pd.Timestamp.now().year - ages_floor
            result[mask] = pd.to_datetime(
                birth_years.astype(str) + "-01-01",
                format="%Y-%m-%d",
                errors="coerce",
            )

        # Type string
        elif transformation.type_transformation == TypeTransformation.UPPER:
            result[mask] = series[mask].astype(str).str.upper()

        elif transformation.type_transformation == TypeTransformation.LOWER:
            result[mask] = series[mask].astype(str).str.lower()

        elif transformation.type_transformation == TypeTransformation.REPLACE:
            result[mask] = series[mask].replace(
                transformation.value_str, transformation.value_str_2
            )

        elif transformation.type_transformation == TypeTransformation.REGEX_REPLACE:
            result[mask] = (
                series[mask]
                .astype(str)
                .str.replace(
                    transformation.value_str, transformation.value_str_2, regex=True
                )
            )

        elif transformation.type_transformation == TypeTransformation.SUBSTRING:
            result[mask] = (
                series[mask]
                .astype(str)
                .str.slice(transformation.value_int, transformation.value_int_2)
            )

        # Type datetime
        elif transformation.type_transformation == TypeTransformation.ADD_DAYS:
            # Ajouter un nombre de jours (int) à la date
            result[mask] = series[mask] + pd.to_timedelta(
                transformation.value_int, unit="d"
            )

        elif transformation.type_transformation == TypeTransformation.ADD_MONTH:
            # Ajouter un nombre de mois (int) à la date
            result[mask] = series[mask] + pd.DateOffset(months=transformation.value_int)

        elif transformation.type_transformation == TypeTransformation.ADD_YEAR:
            # Ajouter un nombre d'années (int) à la date
            result[mask] = series[mask] + pd.DateOffset(years=transformation.value_int)

        elif transformation.type_transformation == TypeTransformation.EXTRACT_DAY:
            # Extraire le jour
            result[mask] = series[mask].dt.day

        elif transformation.type_transformation == TypeTransformation.EXTRACT_MONTH:
            # Extraire le mois
            result[mask] = series[mask].dt.month

        elif transformation.type_transformation == TypeTransformation.EXTRACT_YEAR:
            # Extraire l'année
            result[mask] = series[mask].dt.year

        elif transformation.type_transformation == TypeTransformation.DEFAULT_DAY:
            # Remplacer le jour par une valeur par défaut (int)
            result[mask] = series[mask].apply(
                lambda x: x.replace(day=transformation.value_int)
            )

        elif transformation.type_transformation == TypeTransformation.DEFAULT_MONTH:
            # Remplacer le mois par une valeur par défaut (int)
            result[mask] = series[mask].apply(
                lambda x: x.replace(month=transformation.value_int)
            )

        elif transformation.type_transformation == TypeTransformation.DEFAULT_YEAR:
            # Remplacer l'année par une valeur par défaut (int)
            result[mask] = series[mask].apply(
                lambda x: x.replace(year=transformation.value_int)
            )

        # --- Ajout d'heures, minutes, secondes ---
        elif transformation.type_transformation == TypeTransformation.ADD_HOUR:
            result[mask] = series[mask] + pd.to_timedelta(
                transformation.value_int, unit="h"
            )

        elif transformation.type_transformation == TypeTransformation.ADD_MINUTE:
            result[mask] = series[mask] + pd.to_timedelta(
                transformation.value_int, unit="m"
            )

        elif transformation.type_transformation == TypeTransformation.ADD_SECOND:
            result[mask] = series[mask] + pd.to_timedelta(
                transformation.value_int, unit="s"
            )

        # --- Extraction d'heure, minute, seconde ---
        elif transformation.type_transformation == TypeTransformation.EXTRACT_HOUR:
            result[mask] = series[mask].dt.hour

        elif transformation.type_transformation == TypeTransformation.EXTRACT_MINUTE:
            result[mask] = series[mask].dt.minute

        elif transformation.type_transformation == TypeTransformation.EXTRACT_SECOND:
            result[mask] = series[mask].dt.second

        # Type array
        elif transformation.type_transformation == TypeTransformation.ARRAY_UNIQUE:
            result[mask] = series[mask].apply(
                lambda x: list(dict.fromkeys(x)) if isinstance(x, list) else x
            )

        elif transformation.type_transformation == TypeTransformation.ARRAY_SLICE:
            result[mask] = series[mask].apply(
                lambda x: (
                    x[transformation.value_int : transformation.value_int_2]
                    if isinstance(x, list)
                    else x
                )
            )

        return result

    except Exception:
        series[mask] = np.nan
        return series


def get_expected_type(transformation: ETLColumnTransformation) -> str:
    """Retourne le type de travail attendu selon la transformation."""
    transfo_type = transformation.type_transformation

    if transfo_type in {
        TypeTransformation.MULTIPLY,
        TypeTransformation.DIVIDE,
        TypeTransformation.ADD,
        TypeTransformation.SUBTRACT,
        TypeTransformation.ROUND,
        TypeTransformation.CLIP_MAX,
        TypeTransformation.CLIP_MIN,
        TypeTransformation.POWER,
        TypeTransformation.CEIL,
        TypeTransformation.FLOOR,
        TypeTransformation.AGE_TO_BIRTHDATE,
    }:
        return "numeric"

    if transfo_type in {
        TypeTransformation.UPPER,
        TypeTransformation.LOWER,
        TypeTransformation.REPLACE,
        TypeTransformation.REGEX_REPLACE,
        TypeTransformation.SUBSTRING,
    }:
        return "string"

    if transfo_type in {
        TypeTransformation.ADD_DAYS,
        TypeTransformation.ADD_MONTH,
        TypeTransformation.ADD_YEAR,
        TypeTransformation.EXTRACT_DAY,
        TypeTransformation.EXTRACT_MONTH,
        TypeTransformation.EXTRACT_YEAR,
        TypeTransformation.DEFAULT_DAY,
        TypeTransformation.DEFAULT_MONTH,
        TypeTransformation.DEFAULT_YEAR,
        TypeTransformation.ADD_HOUR,
        TypeTransformation.ADD_MINUTE,
        TypeTransformation.ADD_SECOND,
        TypeTransformation.EXTRACT_HOUR,
        TypeTransformation.EXTRACT_MINUTE,
        TypeTransformation.EXTRACT_SECOND,
    }:
        return "datetime"

    if transfo_type in {
        TypeTransformation.ARRAY_UNIQUE,
        TypeTransformation.ARRAY_SLICE,
    }:
        return "array"

    return "string"


def safe_parse_array(value: object) -> list | float:
    """Parse un tableau JSON ou Python littéral en objet list."""
    if pd.isna(value):
        return np.nan

    if isinstance(value, list):
        return value

    try:
        return json.loads(value)
    except Exception:  # noqa: BLE001
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            return np.nan


def convert_series(series: pd.Series, target_type: str) -> pd.Series:
    """Convertit une série vers un type de travail interne."""
    if target_type == "numeric":
        return pd.to_numeric(series, errors="coerce").astype(float)
    if target_type == "datetime":
        return pd.to_datetime(series, errors="coerce")
    if target_type == "array":
        return series.apply(safe_parse_array)
    if target_type == "string":
        return series.astype(str)
    return series


def apply_transformations(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    mappings: list[ETLColumnMapping],
    df_original: pd.DataFrame = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Applique toutes les transformations définies dans les mappings sur un DataFrame.

    Pipeline par transformation :
    1. Clean string
    2. Conversion type cible (coerce → NaN)
    3. Détection erreurs de conversion → anomalies
    4. Conditions
    5. Transformation
    6. Retour en string
    7. Clean
    8. Détection erreurs post-transfo → anomalies
    """

    # Copie de travail
    df_clean = df.copy()

    # Copie originale pour log anomalies (important : jamais modifiée)
    if df_original is None:
        df_original = df.copy()

    # Masque global des lignes déjà en erreur (exclues du pipeline)
    error_mask_global = pd.Series(False, index=df_clean.index)

    # BOUCLE SUR LES COLONNES
    for mapping in mappings:
        col = mapping.colonne_bdd

        if col not in df_clean.columns:
            continue
        if not mapping.transformations:
            continue

        # Lignes actives = non nulles + pas déjà en erreur
        active_mask = ~df_clean[col].isna() & ~error_mask_global

        # Tri des transformations
        transformations_sorted = sorted(mapping.transformations, key=lambda t: t.ordre)

        # BOUCLE SUR LES TRANSFORMATIONS
        for transformation in transformations_sorted:
            working_mask = active_mask.copy()

            if not working_mask.any():
                break

            # 1. CLEAN STRING
            df_clean[[col]] = clean_txt(df_clean[[col]])

            # Met à jour le masque après nettoyage
            working_mask = working_mask & ~df_clean[col].isna()

            if not working_mask.any():
                break

            # 2. CONVERSION TYPE CIBLE
            target_type = get_expected_type(transformation)

            # Conversion SAFE (erreurs → NaN)
            converted = convert_series(df_clean[col], target_type)

            # 3. DETECTION ERREURS CONVERSION
            # On détecte uniquement les nouvelles erreurs (pas les NaN d'origine)
            conversion_error_mask = (
                working_mask & converted.isna() & ~df_clean[col].isna()
            )

            if conversion_error_mask.any():
                anomalies = add_to_anomalies(
                    df_clean,
                    df_original,
                    anomalies,
                    conversion_error_mask,
                    f"Erreur conversion type col {col} transfo {transformation.ordre}",
                )

                # On exclut ces lignes du pipeline
                active_mask = active_mask & ~conversion_error_mask
                error_mask_global |= conversion_error_mask

            # Mise à jour du working_mask après exclusion
            working_mask = active_mask.copy()

            if not working_mask.any():
                break

            # 4. EVALUATION CONDITIONS
            # Conditions évaluées sur la version typée
            condition_mask = evaluate_conditions(converted, transformation.conditions)

            # Sécurité : NaN dans condition → False
            condition_mask = condition_mask.fillna(False)

            condition_mask &= working_mask

            # Lignes qui échouent les conditions
            fail_mask = working_mask & ~condition_mask

            # Gestion comportement (SKIP / STOP / ERROR)
            active_mask, error_mask_global, anomalies = handle_condition_failure(
                df_clean,
                df_original,
                anomalies,
                transformation,
                col,
                active_mask,
                error_mask_global,
                fail_mask,
            )

            # Recalcul working_mask après gestion
            working_mask = active_mask.copy()

            if not working_mask.any():
                break

            # 5. LIGNES A TRANSFORMER
            success_mask = working_mask & condition_mask

            if not success_mask.any():
                continue

            # Sauvegarde AVANT transformation
            previous_values = df_clean[col].copy()

            # 6. TRANSFORMATION
            transformed = apply_transformation(converted, transformation, success_mask)

            # 7. RETOUR EN STRING
            df_clean.loc[success_mask, col] = (
                transformed[success_mask]
                .astype(str)
                .replace("nan", np.nan)  # évite string "nan"
            )

            # 8. CLEAN POST-TRANSFORMATION
            df_clean[[col]] = clean_txt(df_clean[[col]])

            # 9. DETECTION ERREURS POST-TRANSFO
            new_error_mask = (
                success_mask & df_clean[col].isna() & ~previous_values.isna()
            )

            if new_error_mask.any():
                anomalies = add_to_anomalies(
                    df_clean,
                    df_original,
                    anomalies,
                    new_error_mask,
                    f"Erreur transformation col {col} transfo {transformation.ordre}",
                )

                # Exclusion des lignes en erreur
                active_mask = active_mask & ~new_error_mask
                error_mask_global |= new_error_mask

        # Fin transformations colonne

    # SUPPRESSION LIGNES EN ERREUR
    df_clean = df_clean[~error_mask_global].copy()

    return df_clean, anomalies
