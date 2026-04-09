# HealthAI - Module ETL (Extract, Transform, Load)

Ce repository contient le pipeline de données de l'écosystème **HealthAI**. Son rôle est de récupérer les données de santé (APIs externes, fichiers JSON/CSV), de les normaliser et de les injecter dans la base de données PostgreSQL commune.

## Stack Technique

- **Langage :** Python 3.12+
- **Gestionnaire de paquets :** [uv](https://docs.astral.sh/uv/) (Ultra-rapide, remplace pip/poetry)
- **ORM :** SQLAlchemy 2.0 (Mapping Objet-Relationnel)
- **Migrations :** Alembic (Versionnage du schéma de base de données)
- **Environnement :** Docker & Docker-Compose

## Structure du Projet

```text
├── alembic/              # Scripts de migrations SQL
├── data/                 # Stockage local des données (raw/clean)
├── src/
│   ├── data_pipeline/
│   │   ├── downloader/   # Récupération des données sources
│   │   ├── harmonize/    # Nettoyage et transformation (Logique métier)
│   │   ├── loader/       # Insertion en base de données
│   │   ├── database.py   # Configuration de la connexion SQLAlchemy
│   │   └── models.py     # Définition des tables (Modèles ORM)
│   └── main.py           # Point d'entrée du pipeline
├── .env                  # Variables d'environnement
├── docker-compose.yml    # Infrastructure locale (Postgres)
└── pyproject.toml        # Dépendances du projet (gérées par uv)
```

## Workflow Developpement

1. **Dev Local** En gros on taff en local pour tester rapidement avec uv sur nos machines pour coder et tester, la base de données est sur docker.

2. **Livraison** Lorsque c'est validé -> création et déploiement d'une image de L'ETL.

## Installation et Démarrage.

### prérequis

Installer UV sur la machine :
powershell -ExecutionPolicy ByPass -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex

### configuration

Créez un fichier .env  
avec les variables d'environnement pour la connexion à la base de données :
DATABASE_URL=postgresql://healthai:healthai_pass@localhost:5432/healthai_db

### Lancer la base de données

Docker-compose up -d

### Installer les dépendances et synchroniser

uv sync

### appliquer les migrations

uv run alembic upgrade head

### Exécuter le pipeline

Avant d'exécuter la ligne suivante entrer dans le terminal -> $env:PYTHONPATH = "."

uv run python src/main.py
-> structuration de la base et test d'insertion de donnée dans la table utilisateur et profil_sante

### Pour le Downloader

Récupérer la clé API sur kaggle créer son compte et aller dans les paramètres , récupérer également son username placer les crédentials dans .env
Pour les exercices il faut créer un compte sur https://rapidapi.com/hub pour avoir une clé API (suivre le env.example)
se rendre sur API EDB : https://rapidapi.com/ascendapi/api/edb-with-videos-and-images-by-ascendapi/playground/apiendpoint_bafbc96b-3f58-4a76-aad0-6f8bc44d3afb

### Notebook

Pour lire les données recueillies dans Raw et faire une analyse
Selectionner le kernel en haut a droite python 3.13 (stable pour utiliser pandas etc) pour pouvoir activer les cellules. UV nous a proposé directement la version 3.14 de python mais elle n'est pas stable je m'en suis rendu compte j'ai utilisé la commande : uv python pin 3.13 fermé vscode et ouvert et selectionné le kernel 3.13

uv run python src/data_pipeline/downloader/api_client.py -> fichiers dans raw

### Linter Ruff

uv run ruff format --check -> check le format
uv run ruff format . -> formate le code
uv run pytest -> run les test

### Server
