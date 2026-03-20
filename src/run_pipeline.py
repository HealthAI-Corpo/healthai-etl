"""Pipeline ETL principal — placeholder M1.

Ce script sera complété lors du milestone M1 :
  - ingest : lecture des datasets bruts (data/raw/)
  - clean  : nettoyage, détection d'anomalies, tag_status()
  - load   : INSERT dans PostgreSQL (status = 'validated' | 'draft')
"""

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    logger.info("Pipeline ETL démarré (placeholder M1)")
    # TODO M1 : ingest_nutrition, ingest_users, ingest_exercises
    # TODO M1 : clean_utils.tag_status()
    # TODO M1 : load_to_db.insert_prod()
    logger.info("Pipeline ETL terminé")


if __name__ == "__main__":
    run_pipeline()
