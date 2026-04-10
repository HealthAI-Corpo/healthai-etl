from src.data_pipeline.downloader import get_df_matched_files
from src.data_pipeline.harmonize import (
    apply_transformations,
    check_column_constraint,
    clean_txt,
    column_mapper,
    convert_column_type,
    generate_anomaly_dataframe,
    handle_missing_values,
)
from src.data_pipeline.loader import loader_pipeline
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


def execute_pipeline_etl(pipeline: PipelineETL, override_path: str = None) -> list[str]:
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
        df_clean = column_mapper(df, pipeline_column_mapping)
        anomalies = generate_anomaly_dataframe(pipeline_column_mapping)

        df_clean = clean_txt(df_clean)
        df_clean, anomalies = apply_transformations(
            df_clean, anomalies, pipeline_column_mapping
        )
        df_clean, anomalies = handle_missing_values(
            df_clean, anomalies, pipeline_column_mapping
        )
        df_clean, anomalies = convert_column_type(
            df_clean, anomalies, pipeline_column_mapping
        )
        df_clean, anomalies = check_column_constraint(
            df_clean, anomalies, pipeline_column_mapping
        )

        path, _ = loader_pipeline(
            df_clean,
            anomalies,
            pipeline,
            source_path=source_path,
        )
        output_paths.append(path)

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
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(2, min_length=0, max_length=100),
        transformations=[],
    )

    col_muscles_principaux = ETLColumnMapping(
        id_etl_column_mapping=3,
        colonne_bdd="muscles_principaux",
        colonne_fichier="targetMuscles",
        in_file=True,
        type_donnees=TypeDonnees.ARRAY_DELIMITED_JSON,
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(3, min_length=0, max_length=100),
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
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(4, min_length=0, max_length=100),
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
        nullable=False,
        valeur_defaut=None,
        unique_constraint=False,
        constraint=StringConstraint(5, min_length=0, max_length=100),
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
        dossier_emplacement="\\raw\\",
        nom_fichier_fixe="exercisedb_hobby",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.JSON,
        dossier_clean_emplacement="/clean",
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
        id_etl_column_mapping=20, colonne_bdd="nom", 
        colonne_fichier="Food_Item", in_file=True,
        type_donnees=TypeDonnees.STRING, nullable=False,
        valeur_defaut=None,          
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=250)
    )
    col_category = ETLColumnMapping(
        id_etl_column_mapping=21, colonne_bdd="categorie", 
        colonne_fichier="Category", in_file=True,
        type_donnees=TypeDonnees.STRING, nullable=True,
        valeur_defaut=None,        
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=100),
    )
    col_meal = ETLColumnMapping(
        id_etl_column_mapping=22, colonne_bdd="type_repas", 
        colonne_fichier="Meal_Type", in_file=True,
        type_donnees=TypeDonnees.STRING, nullable=True,
        valeur_defaut=None,          
        unique_constraint=False,
        constraint=StringConstraint(20, min_length=1, max_length=50),
    )

    # --- MACRONUTRIMENTS (DECIMAL) ---
    col_calories = ETLColumnMapping(
        id_etl_column_mapping=23, colonne_bdd="calories", 
        colonne_fichier="Calories (kcal)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=24, colonne_bdd="proteines", 
        colonne_fichier="Protein (g)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=25, colonne_bdd="glucides", 
        colonne_fichier="Carbohydrates (g)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=26, colonne_bdd="lipides", 
        colonne_fichier="Fat (g)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=27, colonne_bdd="fibres", 
        colonne_fichier="Fiber (g)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=28, colonne_bdd="sucres", 
        colonne_fichier="Sugars (g)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=29, colonne_bdd="sodium_mg", 
        colonne_fichier="Sodium (mg)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=30, colonne_bdd="cholesterol_mg", 
        colonne_fichier="Cholesterol (mg)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        id_etl_column_mapping=31, colonne_bdd="eau_ml", 
        colonne_fichier="Water_Intake (ml)", in_file=True,
        type_donnees=TypeDonnees.DECIMAL, nullable=True,
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
        dossier_emplacement="\\raw\\",
        nom_fichier_fixe="daily_food_nutrition_dataset",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="/clean",
        active=True,
        colonnes=[
            col_food, col_category, col_meal, col_calories, 
            col_protein, col_carbs, col_fat, col_fiber, 
            col_sugar, col_sodium, col_cholesterol, col_water
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)


def execute_pipeline_diet_recommendations_dataset(file_path: str = None) -> list[str]:
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
        transformations=[],
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
        constraint=NumericConstraint(1, nb_min=0, nb_max=250, nb_decimal=0),
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
        colonne_bdd="tension_arterielle_mmHg",
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
                ordre=1,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str ="FEMALE",
                value_str_2 = "F",
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
                    )
                ]
            ),
            ETLColumnTransformation(
                id_transformation=5,
                id_etl_column_mapping=3,
                ordre=1,
                type_transformation=TypeTransformation.REPLACE,
                condition_fail_behavior=ConditionFailBehavior.ERROR,
                value_str ="MALE",
                value_str_2 = "M"
            )
        ]
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
        dossier_emplacement="\\raw\\",
        nom_fichier_fixe="diet_recommendations_dataset",
        nom_fichier_variable="",
        extension_fichier=ExtensionFichier.CSV,
        dossier_clean_emplacement="/clean",
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
            col_recommendation_regime
        ],
    )

    return execute_pipeline_etl(pipeline, override_path=file_path)