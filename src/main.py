from data_pipeline.pipeline import (
    execute_pipeline_exercisedb_hobby, 
    execute_pipeline_daily_food, 
    execute_pipeline_diet_recommendations_dataset,
    execute_pipeline_profil_sante,
    execute_pipeline_dataset_historique_seance_exercice,
    execute_pipeline_dataset_historique_seance_exercice_synthetic_data,
)
from src.data_pipeline.database import engine, Base

from src.data_pipeline.models import (
    Aliment,
    Exercice,
    Utilisateur,
    ProfilSante,
    DatasetRecommendationsRegime,
    DatasetHistoriqueSeanceExercice,
)


def run_all_pipelines():
    print("Démarrage de la suite ETL complète...")

    # Pipeline Exercices
    results_exercises = execute_pipeline_exercisedb_hobby()
    print(f"Exercices traités : {results_exercises}")

    # Pipeline diet_recommendations_dataset
    results_diet_recommendations_dataset = (
        execute_pipeline_diet_recommendations_dataset(rename_source=False)
    )
    print(
        f"diet_recommendations_dataset traités : {results_diet_recommendations_dataset}"
    )

    # Pipeline pipeline_daily_food
    results_pipeline_daily_food = execute_pipeline_daily_food()
    print(f"pipeline_daily_food traités : {results_pipeline_daily_food}")
    
    # Pipeline historique_seance_exercice
    results_historique_seance_exercice = execute_pipeline_dataset_historique_seance_exercice()
    print(f"dataset_historique_seance_exercice traités : {results_historique_seance_exercice}")
    # Pipeline historique_seance_exercice
    results_historique_seance_exercice_synthetic_data = execute_pipeline_dataset_historique_seance_exercice_synthetic_data()
    print(f"dataset_historique_seance_exercice_synthetic_data traités : {results_historique_seance_exercice_synthetic_data}")

    # Pipeline profil_sante
    results_profil_sante = execute_pipeline_profil_sante()
    print(f"profil_sante traités : {results_profil_sante}")

    # Ajouter les autres traitement
    # execute_pipeline_users()


def init_db():
    print("♻️ Réinitialisation de la base de données...")

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    print("Base de données prête (tables vides).")


if __name__ == "__main__":
    # Optionnel : réinitialisation de la DB pour tes tests
    # init_db()
    run_all_pipelines()
