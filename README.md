# HealthAI - Module ETL (Extract, Transform, Load)

Ce repository contient le pipeline de données de l'écosystème **HealthAI**. Son rôle est de récupérer les données de santé (APIs externes, fichiers JSON/CSV), de les normaliser et de les injecter dans la base de données PostgreSQL commune.

## Stack Technique

- **Langage :** Python 3.12+
- **Gestionnaire de paquets :** [uv](https://docs.astral.sh/uv/) (remplace pip/poetry)
- **ORM :** SQLAlchemy 2.0
- **Serveur :** FastAPI + Uvicorn
- **Tests :** pytest
- **Lint :** Ruff
- **Environnement :** Docker (image GHCR)

## Structure du Projet

```text
├── alembic/              # Migrations Alembic (référence, schéma géré par init.sql)
├── data/
│   ├── raw/              # Données sources (gitignored)
│   └── clean/            # Données nettoyées (gitignored)
├── src/
│   ├── data_pipeline/
│   │   ├── downloader/   # Lecture fichiers CSV/JSON
│   │   ├── harmonize/    # Nettoyage, transformations, validation contraintes
│   │   ├── loader/       # Insertion en base de données
│   │   ├── utils/        # Config pipeline (dataclasses, enums)
│   │   ├── database.py   # Connexion SQLAlchemy (lazy init)
│   │   ├── models.py     # Modèles ORM (source de vérité du schéma)
│   │   └── pipeline.py   # Orchestrateur ETL
│   ├── server.py         # API FastAPI (POST /upload, GET /health)
│   ├── run_pipeline.py   # Point d'entrée batch (CMD Docker)
│   └── main.py           # Script de test dev local
├── tests/
├── pyproject.toml        # Dépendances (gérées par uv)
└── uv.lock
```

## Installation locale

### Prérequis

Installer uv :

```powershell
# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

```bash
# Linux/Mac
curl -LsSf https://astral.sh/uv | sh
```

### Configuration

Créer un fichier `.env` à la racine :

```env
DATABASE_URL=postgresql://healthai:healthai_secret@localhost:5432/healthai_db
```

### Démarrage

```bash
# Démarrer la base de données (depuis le repo MSPRHealthAI)
docker compose up db -d

# Installer les dépendances
uv sync

# Lancer les tests
uv run pytest tests/ -v

# Lancer le serveur FastAPI
uv run uvicorn src.server:app --reload

# Lancer le pipeline batch (dev)
uv run python src/run_pipeline.py
```

> **Note :** Le schéma de base de données est géré par `db/init.sql` dans le repo `MSPRHealthAI`.
> Les migrations Alembic (`alembic/`) sont conservées comme référence mais ne sont pas appliquées en production.

## Données sources

Placer les fichiers dans `data/raw/` (non commités) :

| Fichier                 | Source                                |
| ----------------------- | ------------------------------------- |
| `exercisedb_hobby.json` | API ExerciseDB (via `api_client.py`)  |
| `*.csv`                 | Kaggle (voir credentials dans `.env`) |

Pour récupérer les données Kaggle : ajouter `KAGGLE_USERNAME` et `KAGGLE_KEY` dans `.env`.

Pour les exercices (RapidAPI) : ajouter `RAPIDAPI_KEY` dans `.env`.

## CI / CD

- **Lint** (Ruff) : sur push `develop`/`main` et PR vers `main`
- **Tests** (pytest) : idem, avec PostgreSQL en service
- **Build & Push** : sur push `main` uniquement → `ghcr.io/healthai-corpo/healthai-etl:latest`
