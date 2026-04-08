from data_pipeline.pipeline import execute_pipeline_exercisedb_hobby
from data_pipeline.database import SessionLocal, engine, Base

if __name__ == "__main__":
    print("♻️ Réinitialisation de la base de données...")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    print("Lancement de l'ETL")
    test = execute_pipeline_exercisedb_hobby()
    print("Fin de l'ETL")
