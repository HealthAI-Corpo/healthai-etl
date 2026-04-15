from src.data_pipeline.pipeline import run_all_pipelines
from src.data_pipeline.database import engine, Base
from src.utils.logger import configure_logging, logger
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration du logging au démarrage
configure_logging()


def init_db():
    logger.info("Réinitialisation de la base de données...")

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    logger.info("Base de données prête (tables vides)")


if __name__ == "__main__":
    logger.info("Démarrage de l'application ETL")
    try:
        # init_db()
        run_all_pipelines()
        logger.info("Exécution complète des pipelines terminée")
    except Exception as e:
        logger.error(
            "Erreur fatale lors de l'exécution des pipelines : {}",
            str(e),
            exc_info=True,
        )
        raise
