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
)
from src.data_pipeline.downloader.file_reader import read_single_file_with_pandas


def execute_pipeline_etl(pipeline: PipelineETL, override_path: str = None) -> list[str]:
    """Execute the ETL flow for a pipeline definition and return output clean file paths."""
    pipeline_column_mapping = pipeline.colonnes
    dfs_matched_files = get_df_matched_files(pipeline)

    # LOGIQUE DE SÉLECTION DE LA SOURCE
    if override_path:
        # Cas API : On lit directement le fichier uploadé
        df_to_process = read_single_file_with_pandas(override_path)
        dfs_matched_files = [df_to_process] if df_to_process is not None else []
    else:
        # Cas CRON : On scanne le dossier selon la config du pipeline
        dfs_matched_files = get_df_matched_files(pipeline)

    output_paths: list[str] = []

    for df in dfs_matched_files:
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

        path = loader_pipeline(df_clean, anomalies, pipeline)
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
