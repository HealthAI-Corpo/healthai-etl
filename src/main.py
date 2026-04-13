from src.data_pipeline.pipeline import (
    run_all_pipelines,
)
from src.data_pipeline.database import engine, Base


def init_db():
    print("♻️ Réinitialisation de la base de données...")

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    print("Base de données prête (tables vides).")


if __name__ == "__main__":
    # init_db()
    run_all_pipelines()
