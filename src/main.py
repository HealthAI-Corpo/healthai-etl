from data_pipeline.pipeline import execute_pipeline_exercisedb_hobby


def run_all_pipelines():
    print("Démarrage de la suite ETL complète...")

    # Pipeline Exercices
    results_exercises = execute_pipeline_exercisedb_hobby()
    print(f"Exercices traités : {results_exercises}")

    # Ajouter les autres traitement
    # execute_pipeline_nutrition()
    # execute_pipeline_users()


if __name__ == "__main__":
    # Optionnel : réinitialisation de la DB pour tes tests
    # init_db()
    run_all_pipelines()
