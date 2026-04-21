import os

from src.data_pipeline.downloader.api_client import run_downloader

from src.data_pipeline.downloader import get_df_matched_files
from src.data_pipeline.harmonize import (
    apply_transformations,
    clean_txt,
    column_mapper,
    generate_anomaly_dataframe,
    validate_and_clean_data,
    validate_required_columns,
)
from src.data_pipeline.loader import loader_pipeline, log_etl_validation_error
from src.data_pipeline.utils import (
    ConditionFailBehavior,
    ETLColumnMapping,
    ETLColumnTransformation,
    ExtensionFichier,
    PipelineETL,
    StringConstraint,
    TypeDonnees,
    TypeTransformation,
    NumericConstraint,
    ETLTransformationCondition,
    ConditionOperator,
)
from src.data_pipeline.downloader.file_reader import read_single_file_with_pandas
from src.utils.logger import logger


def execute_pipeline_etl(
    pipeline: PipelineETL,
    override_path: str = None,
) -> list[str]:
    """Execute the ETL flow for a pipeline definition and return output clean file paths."""
    pipeline_column_mapping = pipeline.colonnes

    # LOGIQUE DE SÉLECTION DE LA SOURCE
    if override_path:
        # Cas API : On lit directement le fichier uploadé
        df_to_process = read_single_file_with_pandas(override_path)
        files_with_df = (
            [(override_path, df_to_process)] if df_to_process is not None else []
        )
    else:
        # Cas CRON : On scanne le dossier selon la config du pipeline
        files_with_df = get_df_matched_files(pipeline)

    output_paths: list[str] = []

    for source_path, df in files_with_df:
        try:
            # VALIDATION: Vérifier que toutes les colonnes obligatoires existent
            is_valid, missing_columns = validate_required_columns(
                df, pipeline_column_mapping
            )
            if not is_valid:
                error_msg = (
                    f"Colonnes obligatoires manquantes dans le fichier {source_path}: "
                    f"{', '.join(missing_columns)}"
                )
                raise ValueError(error_msg)

            # Colonnes à exclure
            cols_a_supprimer = ["_row_id", "erreur"]

            # Garder le DF original sans ces colonnes
            df_original = df.drop(columns=cols_a_supprimer, errors="ignore").copy()

            # Ajouter une colonne de traçage pour maintenir l'alignement après reset_index()
            df_original["_row_id"] = range(len(df_original))

            df_clean = column_mapper(df, pipeline_column_mapping)
            # Ajouter la même colonne de traçage à df_clean
            df_clean["_row_id"] = range(len(df_clean))

            anomalies = generate_anomaly_dataframe(df_original.columns)

            df_clean = clean_txt(df_clean)
            df_clean, anomalies = apply_transformations(
                df_clean, anomalies, pipeline_column_mapping, df_original
            )
            df_clean, anomalies = validate_and_clean_data(
                df_clean, anomalies, pipeline_column_mapping, df_original
            )

            # Enlever la colonne de traçage avant de sauvegarder
            df_clean = df_clean.drop(columns=["_row_id"], errors="ignore")
            anomalies = anomalies.drop(columns=["_row_id"], errors="ignore")

            path, _ = loader_pipeline(
                df_clean,
                anomalies,
                pipeline,
                source_path=source_path,
            )
            output_paths.append(path)
        except ValueError as e:
            # Erreur de validation (ex: colonnes manquantes)
            error_msg = str(e)
            logger.error(error_msg)
            log_etl_validation_error(
                libelle_pipeline=pipeline.libelle,
                fichier_nom=os.path.basename(source_path) if source_path else "unknown",
                error_message=error_msg,
                source_path=source_path,
            )
        except Exception:
            logger.exception(f"Erreur lors du traitement du fichier {source_path}")

    return output_paths


def execute_pipeline_exercisedb_hobby(file_path: str = None) -> list[str]:
    """Build a PipelineETL config for exercisedb_hobby import."""
    col_nom = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="nom",
        colonne_fichier="name",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=150),
        transformations=[],
    )

    col_exercise_type = ETLColumnMapping(
        id_etl_column_mapping=2,
        colonne_bdd="type_exercice",
        colonne_fichier="exerciseType",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(4, min_length=0, max_length=500),
        transformations=[],
    )

    col_muscles_principaux = ETLColumnMapping(
        id_etl_column_mapping=3,
        colonne_bdd="muscles_principaux",
        colonne_fichier="targetMuscles",
        in_file=True,
        type_donnees=TypeDonnees.ARRAY_DELIMITED_JSON,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(4, min_length=0, max_length=1500),
        transformations=[
            ETLColumnTransformation(
                id_transformation=1,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ARRAY_UNIQUE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            )
        ],
    )

    col_muscles_secondaires = ETLColumnMapping(
        id_etl_column_mapping=4,
        colonne_bdd="muscles_secondaires",
        colonne_fichier="secondaryMuscles",
        in_file=True,
        type_donnees=TypeDonnees.ARRAY_DELIMITED_JSON,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(2, min_length=0, max_length=1500),
        transformations=[
            ETLColumnTransformation(
                id_transformation=2,
                id_etl_column_mapping=4,
                ordre=1,
                type_transformation=TypeTransformation.ARRAY_UNIQUE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            )
        ],
    )

    col_equipement = ETLColumnMapping(
        id_etl_column_mapping=5,
        colonne_bdd="equipement",
        colonne_fichier="equipments",
        in_file=True,
        type_donnees=TypeDonnees.ARRAY_DELIMITED_JSON,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(5, min_length=0, max_length=1500),
        transformations=[
            ETLColumnTransformation(
                id_transformation=3,
                id_etl_column_mapping=5,
                ordre=1,
                type_transformation=TypeTransformation.ARRAY_UNIQUE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            )
        ],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=1,
        libelle="Import exercisedb_hobby",
        table_nom="exercice",
        dossier_emplacement="raw",
        nom_fichier_fixe="exercisedb_hobby",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.JSON,
        dossier_clean_emplacement="clean",
        active=True,
        colonnes=[
            col_nom,
            col_exercise_type,
            col_muscles_principaux,
            col_equipement,
            col_muscles_secondaires,
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def execute_pipeline_daily_food(file_path: str = None) -> list[str]:
    """Build a PipelineETL config with ALL columns from daily_food_nutrition_dataset.csv."""

    # --- TEXTE ---
    col_food = ETLColumnMapping(
        id_etl_column_mapping=20,
        colonne_bdd="nom",
        colonne_fichier="Food_Item",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=250),
    )
    col_category = ETLColumnMapping(
        id_etl_column_mapping=21,
        colonne_bdd="categorie",
        colonne_fichier="Category",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=100),
    )
    col_meal = ETLColumnMapping(
        id_etl_column_mapping=22,
        colonne_bdd="type_repas",
        colonne_fichier="Meal_Type",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=50),
    )

    # --- MACRONUTRIMENTS (DECIMAL) ---
    col_calories = ETLColumnMapping(
        id_etl_column_mapping=23,
        colonne_bdd="calories",
        colonne_fichier="Calories (kcal)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )
    col_protein = ETLColumnMapping(
        id_etl_column_mapping=24,
        colonne_bdd="proteines",
        colonne_fichier="Protein (g)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_carbs = ETLColumnMapping(
        id_etl_column_mapping=25,
        colonne_bdd="glucides",
        colonne_fichier="Carbohydrates (g)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_fat = ETLColumnMapping(
        id_etl_column_mapping=26,
        colonne_bdd="lipides",
        colonne_fichier="Fat (g)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    # --- MICRONUTRIMENTS & DÉTAILS ---
    col_fiber = ETLColumnMapping(
        id_etl_column_mapping=27,
        colonne_bdd="fibres",
        colonne_fichier="Fiber (g)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_sugar = ETLColumnMapping(
        id_etl_column_mapping=28,
        colonne_bdd="sucres",
        colonne_fichier="Sugars (g)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_sodium = ETLColumnMapping(
        id_etl_column_mapping=29,
        colonne_bdd="sodium_mg",
        colonne_fichier="Sodium (mg)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_cholesterol = ETLColumnMapping(
        id_etl_column_mapping=30,
        colonne_bdd="cholesterol_mg",
        colonne_fichier="Cholesterol (mg)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )
    col_water = ETLColumnMapping(
        id_etl_column_mapping=31,
        colonne_bdd="eau_ml",
        colonne_fichier="Water_Intake (ml)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=0.0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=3,
        libelle="Import Nutrition Complet",
        table_nom="aliment",
        dossier_emplacement="raw",
        nom_fichier_fixe="daily_food_nutrition_dataset",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="clean",
        active=True,
        colonnes=[
            col_food,
            col_category,
            col_meal,
            col_calories,
            col_protein,
            col_carbs,
            col_fat,
            col_fiber,
            col_sugar,
            col_sodium,
            col_cholesterol,
            col_water,
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def execute_pipeline_diet_recommendations_dataset(
    file_path: str = None,
) -> list[str]:
    """Execute la PipelineETL pour importer le csv diet_recommendations_dataset dans la table dataset_recommendations_regime"""
    col_age = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="age",
        colonne_fichier="Age",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=16, nb_max=100, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.FLOOR,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_poids_kg = ETLColumnMapping(
        id_etl_column_mapping=2,
        colonne_bdd="poids_kg",
        colonne_fichier="Weight_kg",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=250, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_taille_cm = ETLColumnMapping(
        id_etl_column_mapping=3,
        colonne_bdd="taille_cm",
        colonne_fichier="Height_cm",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=140, nb_max=250, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_apport_calorique_journalier = ETLColumnMapping(
        id_etl_column_mapping=4,
        colonne_bdd="apport_calorique_journalier",
        colonne_fichier="Daily_Caloric_Intake",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=500, nb_max=6000, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_cholesterol_mg_dl = ETLColumnMapping(
        id_etl_column_mapping=5,
        colonne_bdd="cholesterol_mg_dl",
        colonne_fichier="Cholesterol_mg/dL",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=50, nb_max=500, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_tension_arterielle_mmHg = ETLColumnMapping(
        id_etl_column_mapping=6,
        colonne_bdd="tension_arterielle_mmhg",
        colonne_fichier="Blood_Pressure_mmHg",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=50, nb_max=500, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_glucose_mg_dl = ETLColumnMapping(
        id_etl_column_mapping=7,
        colonne_bdd="glucose_mg_dl",
        colonne_fichier="Glucose_mg/dL",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=50, nb_max=500, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_heures_exercice_semaine = ETLColumnMapping(
        id_etl_column_mapping=8,
        colonne_bdd="heures_exercice_semaine",
        colonne_fichier="Weekly_Exercise_Hours",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=50, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_adherence_regime = ETLColumnMapping(
        id_etl_column_mapping=9,
        colonne_bdd="adherence_regime",
        colonne_fichier="Adherence_to_Diet_Plan",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=100, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_score_desiquilibre_nutriment = ETLColumnMapping(
        id_etl_column_mapping=10,
        colonne_bdd="score_desiquilibre_nutriment",
        colonne_fichier="Dietary_Nutrient_Imbalance_Score",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=50, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_sexe = ETLColumnMapping(
        id_etl_column_mapping=11,
        colonne_bdd="sexe",
        colonne_fichier="Gender",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=50),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="FEMALE",
                value_str_2="F",
                conditions=[
                    ETLTransformationCondition(
                        id_condition=1,
                        id_transformation=5,
                        groupe_code=1,
                        operator=ConditionOperator.EQ,
                        value_str="FEMALE",
                    ),
                    ETLTransformationCondition(
                        id_condition=2,
                        id_transformation=5,
                        groupe_code=2,
                        operator=ConditionOperator.EQ,
                        value_str="MALE",
                    ),
                ],
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=3,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="MALE",
                value_str_2="M",
            ),
        ],
    )

    col_type_maladie = ETLColumnMapping(
        id_etl_column_mapping=12,
        colonne_bdd="type_maladie",
        colonne_fichier="Disease_Type",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=255),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_gravite = ETLColumnMapping(
        id_etl_column_mapping=13,
        colonne_bdd="gravite",
        colonne_fichier="Severity",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=50),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_restrictions_alimentaires = ETLColumnMapping(
        id_etl_column_mapping=14,
        colonne_bdd="restrictions_alimentaires",
        colonne_fichier="Dietary_Restrictions",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=2, max_length=255),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_allergies = ETLColumnMapping(
        id_etl_column_mapping=15,
        colonne_bdd="allergies",
        colonne_fichier="Allergies",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=2, max_length=255),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_cuisine_preferee = ETLColumnMapping(
        id_etl_column_mapping=16,
        colonne_bdd="cuisine_preferee",
        colonne_fichier="Preferred_Cuisine",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=2, max_length=100),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_niveau_activite_physique = ETLColumnMapping(
        id_etl_column_mapping=17,
        colonne_bdd="niveau_activite_physique",
        colonne_fichier="Physical_Activity_Level",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=100),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_recommendation_regime = ETLColumnMapping(
        id_etl_column_mapping=18,
        colonne_bdd="recommendation_regime",
        colonne_fichier="Diet_Recommendation",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=255),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=1,
        libelle="Import diet_recommendations_dataset",
        table_nom="dataset_recommendations_regime",
        dossier_emplacement="raw",
        nom_fichier_fixe="diet_recommendations_dataset",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="clean",
        active=True,
        colonnes=[
            col_age,
            col_poids_kg,
            col_taille_cm,
            col_apport_calorique_journalier,
            col_cholesterol_mg_dl,
            col_tension_arterielle_mmHg,
            col_glucose_mg_dl,
            col_heures_exercice_semaine,
            col_adherence_regime,
            col_score_desiquilibre_nutriment,
            col_sexe,
            col_type_maladie,
            col_gravite,
            col_restrictions_alimentaires,
            col_allergies,
            col_cuisine_preferee,
            col_niveau_activite_physique,
            col_recommendation_regime,
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def execute_pipeline_profil_sante(file_path: str = None) -> list[str]:
    """Execute la PipelineETL pour peupler la table profil_sante"""

    col_poids_kg = ETLColumnMapping(
        id_etl_column_mapping=101,
        colonne_bdd="poids_kg",
        colonne_fichier="Weight_kg",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=300, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=101,
                id_etl_column_mapping=101,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_taille_cm = ETLColumnMapping(
        id_etl_column_mapping=102,
        colonne_bdd="taille_cm",
        colonne_fichier="Height_cm",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=250, nb_decimal=0),
        transformations=[],
    )

    col_experience_sportive = ETLColumnMapping(
        id_etl_column_mapping=105,
        colonne_bdd="experience_sportive",
        colonne_fichier="Physical_Activity_Level",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=100),
        transformations=[
            ETLColumnTransformation(
                id_transformation=102,
                id_etl_column_mapping=105,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_type_maladie = ETLColumnMapping(
        id_etl_column_mapping=106,
        colonne_bdd="type_maladie",
        colonne_fichier="Disease_Type",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=255),
        transformations=[],
    )

    col_severite = ETLColumnMapping(
        id_etl_column_mapping=107,
        colonne_bdd="severite",
        colonne_fichier="Severity",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=50),
        transformations=[],
    )

    col_restrictions = ETLColumnMapping(
        id_etl_column_mapping=108,
        colonne_bdd="restrictions_alimentaires",
        colonne_fichier="Dietary_Restrictions",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        transformations=[],
    )

    col_allergies = ETLColumnMapping(
        id_etl_column_mapping=109,
        colonne_bdd="allergies",
        colonne_fichier="Allergies",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        transformations=[],
    )

    col_heures_sport = ETLColumnMapping(
        id_etl_column_mapping=110,
        colonne_bdd="heures_entrainement_semaine",
        colonne_fichier="Weekly_Exercise_Hours",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=168, nb_decimal=1),
        transformations=[],
    )

    col_objectif = ETLColumnMapping(
        id_etl_column_mapping=111,
        colonne_bdd="objectif_principal",
        colonne_fichier=None,
        in_file=False,
        type_donnees=TypeDonnees.STRING,
        nullable=True,
        valeur_defaut=None,
        unique_constraint=False,
        transformations=[],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=5,
        libelle="Import Profil Santé",
        table_nom="profil_sante",
        dossier_emplacement="raw",
        nom_fichier_fixe="diet_recommendations_dataset",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="clean",
        active=True,
        colonnes=[
            col_poids_kg,
            col_taille_cm,
            col_experience_sportive,
            col_type_maladie,
            col_severite,
            col_restrictions,
            col_allergies,
            col_heures_sport,
            col_objectif,
        ],
    )

    return execute_pipeline_etl(
        pipeline,
        override_path=file_path,
    )


def execute_pipeline_dataset_historique_seance_exercice(
    file_path: str = None,
) -> list[str]:
    """Execute la PipelineETL pour importer le csv gym_members_exercise_tracking_synthetic_data dans la table dataset_historique_seance_exercice"""
    col_age = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="age",
        colonne_fichier="Age",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=16, nb_max=100, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.FLOOR,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_poids_kg = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="poids_kg",
        colonne_fichier="Weight (kg)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=250, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_taille_cm = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="taille_cm",
        colonne_fichier="Height (m)",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=140, nb_max=250, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=100,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_max = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_max",
        colonne_fichier="Max_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=30, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_moyen = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_moyen",
        colonne_fichier="Avg_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=30, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_repos = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_repos",
        colonne_fichier="Resting_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=20, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_duree_seance_minutes = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="duree_seance_minutes",
        colonne_fichier="Session_Duration (hours)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=360, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=60,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_calories_brulees = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="calories_brulees",
        colonne_fichier="Calories_Burned",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=300, nb_max=3000, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_pourcentage_gras = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="pourcentage_gras",
        colonne_fichier="Fat_Percentage",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=100, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_consommation_eau_ml = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="consommation_eau_ml",
        colonne_fichier="Water_Intake (liters)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=1000,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_frequence_sport_jour_semaine = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="frequence_sport_jour_semaine",
        colonne_fichier="Workout_Frequency (days/week)",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut="0",
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=7, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_niveau_experience = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="niveau_experience",
        colonne_fichier="Experience_Level",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=3, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.CLIP_MAX,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=3,
            ),
        ],
    )

    col_sexe = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="sexe",
        colonne_fichier="Gender",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=50),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="FEMALE",
                value_str_2="F",
                conditions=[
                    ETLTransformationCondition(
                        id_condition=1,
                        id_transformation=5,
                        groupe_code=1,
                        operator=ConditionOperator.EQ,
                        value_str="FEMALE",
                    ),
                    ETLTransformationCondition(
                        id_condition=2,
                        id_transformation=5,
                        groupe_code=2,
                        operator=ConditionOperator.EQ,
                        value_str="MALE",
                    ),
                ],
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=3,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="MALE",
                value_str_2="M",
            ),
        ],
    )

    col_type_sport = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="type_sport",
        colonne_fichier="Workout_Type",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=100),
        transformations=[],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=1,
        libelle="Import gym_members_exercise_tracking",
        table_nom="dataset_historique_seance_exercice",
        dossier_emplacement="\\raw\\",
        nom_fichier_fixe="gym_members_exercise_tracking",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="/clean",
        active=True,
        colonnes=[
            col_age,
            col_poids_kg,
            col_taille_cm,
            col_bpm_max,
            col_bpm_moyen,
            col_bpm_repos,
            col_duree_seance_minutes,
            col_calories_brulees,
            col_pourcentage_gras,
            col_consommation_eau_ml,
            col_frequence_sport_jour_semaine,
            col_niveau_experience,
            col_sexe,
            col_type_sport,
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def execute_pipeline_dataset_historique_seance_exercice_synthetic_data(
    file_path: str = None,
) -> list[str]:
    """Execute la PipelineETL pour importer le csv gym_members_exercise_tracking_synthetic_data dans la table dataset_historique_seance_exercice"""
    col_age = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="age",
        colonne_fichier="Age",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=16, nb_max=100, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=1,
                ordre=1,
                type_transformation=TypeTransformation.REGEX_REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str=r"(?:\\[tnr]|[\t\n\r])+",
                value_str_2="",
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.FLOOR,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
        ],
    )

    col_poids_kg = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="poids_kg",
        colonne_fichier="Weight (kg)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=250, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_taille_cm = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="taille_cm",
        colonne_fichier="Height (m)",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=140, nb_max=250, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=100,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_max = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_max",
        colonne_fichier="Max_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=30, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=1,
                ordre=1,
                type_transformation=TypeTransformation.REGEX_REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str=r"(?:\\[tnr]|[\t\n\r])+",
                value_str_2="",
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_moyen = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_moyen",
        colonne_fichier="Avg_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=30, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_bpm_repos = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="bpm_repos",
        colonne_fichier="Resting_BPM",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=20, nb_max=220, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_duree_seance_minutes = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="duree_seance_minutes",
        colonne_fichier="Session_Duration (hours)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=360, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=60,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_calories_brulees = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="calories_brulees",
        colonne_fichier="Calories_Burned",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=300, nb_max=3000, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_pourcentage_gras = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="pourcentage_gras",
        colonne_fichier="Fat_Percentage",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=100, nb_decimal=1),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=1,
            ),
        ],
    )

    col_consommation_eau_ml = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="consommation_eau_ml",
        colonne_fichier="Water_Intake (liters)",
        in_file=True,
        type_donnees=TypeDonnees.DECIMAL,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=5000, nb_decimal=2),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.MULTIPLY,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=1000,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=2,
            ),
        ],
    )

    col_frequence_sport_jour_semaine = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="frequence_sport_jour_semaine",
        colonne_fichier="Workout_Frequency (days/week)",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut="0",
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=7, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
        ],
    )

    col_niveau_experience = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="niveau_experience",
        colonne_fichier="Experience_Level",
        in_file=True,
        type_donnees=TypeDonnees.INT,
        nullable=False,
        valeur_defaut=0,
        unique_constraint=False,
        constraint=NumericConstraint(1, nb_min=0, nb_max=3, nb_decimal=0),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.ROUND,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_int=0,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.CLIP_MAX,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_num=3,
            ),
        ],
    )

    col_sexe = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="sexe",
        colonne_fichier="Gender",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=50),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.UPPER,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=2,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="FEMALE",
                value_str_2="F",
                conditions=[
                    ETLTransformationCondition(
                        id_condition=1,
                        id_transformation=5,
                        groupe_code=1,
                        operator=ConditionOperator.EQ,
                        value_str="FEMALE",
                    ),
                    ETLTransformationCondition(
                        id_condition=2,
                        id_transformation=5,
                        groupe_code=2,
                        operator=ConditionOperator.EQ,
                        value_str="MALE",
                    ),
                ],
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=3,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str="MALE",
                value_str_2="M",
            ),
        ],
    )

    col_type_sport = ETLColumnMapping(
        id_etl_column_mapping=1,
        colonne_bdd="type_sport",
        colonne_fichier="Workout_Type",
        in_file=True,
        type_donnees=TypeDonnees.STRING,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(1, min_length=0, max_length=100),
        transformations=[
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=1,
                ordre=1,
                type_transformation=TypeTransformation.REGEX_REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str=r"(?:\\[tnr]|[\t\n\r])+",
                value_str_2="",
            ),
        ],
    )

    pipeline = PipelineETL(
        id_etl_pipeline=1,
        libelle="Import gym_members_exercise_tracking_synthetic_data",
        table_nom="dataset_historique_seance_exercice",
        dossier_emplacement="\\raw\\",
        nom_fichier_fixe="gym_members_exercise_tracking_synthetic_data",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="/clean",
        active=True,
        colonnes=[
            col_age,
            col_poids_kg,
            col_taille_cm,
            col_bpm_max,
            col_bpm_moyen,
            col_bpm_repos,
            col_duree_seance_minutes,
            col_calories_brulees,
            col_pourcentage_gras,
            col_consommation_eau_ml,
            col_frequence_sport_jour_semaine,
            col_niveau_experience,
            col_sexe,
            col_type_sport,
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def run_all_pipelines() -> dict:
    """Execute tous les pipelines ETL disponibles et retourne les résultats."""

    try:
        logger.info("Démarrage de la suite ETL complète...")
        run_downloader()
    except Exception:
        logger.exception("Erreur lors du téléchargement des données sources")
        # On continue quand même : si le téléchargement échoue,
        # on peut toujours traiter ce qui est déjà présent dans data/raw
    results = {}

    pipelines = {
        "exercices": execute_pipeline_exercisedb_hobby,
        "diet_recommendations": execute_pipeline_diet_recommendations_dataset,
        "daily_food": execute_pipeline_daily_food,
        "historique_seance": execute_pipeline_dataset_historique_seance_exercice,
        "historique_seance_synthetic": execute_pipeline_dataset_historique_seance_exercice_synthetic_data,
    }

    for name, func in pipelines.items():
        try:
            results[name] = func()
            logger.info(f"Pipeline {name} terminé avec succès → {results[name]}")
        except Exception:
            logger.exception(f"Erreur pipeline {name}")
            results[name] = {"error": True}

    logger.info("Suite ETL complète terminée")

    return results
