from logging.config import fileConfig
import os
from sqlalchemy import engine_from_config, pool
from alembic import context
import sys
from os.path import abspath, dirname

# Ajoute la racine du projet au chemin de recherche de Python
sys.path.insert(0, abspath(dirname(dirname(__file__))))

# Import de ta Base
from src.data_pipeline.database import Base
from src.data_pipeline.models import DatasetHistoriqueSeanceExercice, Utilisateur, ProfilSante, Aliment, Exercice, DatasetRecommendationsRegime, LogAliment, LogSeance, LogSante, EtlLog



config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def include_object(object, name, type_, reflected, compare_to):
    """
    Filtre les objets pour l'autogénération.
    Ignore les tables présentes en BDD mais absentes des modèles SQLAlchemy.
    """
    if type_ == "table" and reflected and compare_to is None:
        return False
    else:
        return True


def run_migrations_offline() -> None:
    url = os.getenv("DATABASE_URL", config.get_main_option("sqlalchemy.url"))
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # On récupère la section de config habituelle
    section = config.get_section(config.config_ini_section, {})
    
    # ON FORCE L'URL : Si DATABASE_URL existe (Docker), on écrase celle du .ini
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        section["sqlalchemy.url"] = env_url

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
