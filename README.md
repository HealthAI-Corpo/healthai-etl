# HealthAI - Module ETL (Extract, Transform, Load)

Ce repository contient le pipeline de données de l'écosystème **HealthAI**. Son rôle est de récupérer les données de santé (APIs externes, fichiers JSON/CSV), de les normaliser et de les injecter dans la base de données PostgreSQL commune.

## Stack Technique

- **Langage :** Python 3.12+
- **Gestionnaire de paquets :** [uv](https://docs.astral.sh/uv/) (Ultra-rapide, remplace pip/poetry)
- **ORM :** SQLAlchemy 2.0 (Mapping Objet-Relationnel)
- **Migrations :** Alembic (Versionnage du schéma de base de données)
- **Environnement :** Docker & Docker-Compose
- **API :** FastAPI (Serveur uvicorn)

## Structure du Projet

```text
├── alembic/ # Scripts de migrations SQL
├── data/ # Stockage local des données (raw/clean)
├── src/
│ ├── data_pipeline/
│ │ ├── downloader/ # Récupération des données sources
│ │ ├── harmonize/ # Nettoyage et transformation (Logique métier)
│ │ ├── loader/ # Insertion en base de données
│ │ ├── database.py # Configuration de la connexion SQLAlchemy
│ │ └── models.py # Définition des tables (Modèles ORM)
│ ├── main.py # Point d'entrée du pipeline
│ └── server.py # Serveur FastAPI (API)
├── cron_kaggle.bat # Automatisation Windows (Planificateur de tâches)
├── cron_kaggle.sh # Automatisation Unix (Linux/Mac)
├── .env # Variables d'environnement
├── docker-compose.yml # Infrastructure complète (App, DB, Metabase)
└── pyproject.toml # Dépendances du projet (gérées par uv)
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
Note : Le docker-compose.yml est configuré pour écraser automatiquement les variables de connexion afin de s'adapter au réseau Docker sans modifier votre .env local.

### Lancement hybride (Développement)

Pour travailler en local il est conseillé de lancer la base de donnée via docker (docker desktop) ou commande Docker-compose up -d db

- Installer les dépendances : uv sync

- Appliquer les migrations : ```bash
  uv run alembic revision --autogenerate -m "description_du_changement"
  uv run alembic upgrade head grâce à la config dans env.py il adapte les types

### Lancer l'infrastructure complète

docker-compose up -d --build

### Automatisation de l'ingestion des données sources via CRON

Pour automatiser la récupération des données Kaggle sans avoir à gérer l'environnement Python du système hôte, des scripts universels sont disponibles à la racine cron_kaggle.bat (windows)cron_kaggle.sh(Linux). Ils lancent les commandes à l'intérieur du container Docker déjà existant.

Sur windows :

- il faut avoir un plannificateur de tâche
- Créer une tâche dans Windows
- Action : Démarrer un programme
- Script : Sélectionner cron_kaggle.bat
- attention: Dans "Démarrer dans", mettre le chemin racine du projet.

Sur linux mac :

- Ajouter une ligne au crontab
- Script : ./cron_kaggle.sh.

### Exécuter le pipeline manuellement

Avant d'exécuter la ligne suivante entrer dans le terminal -> $env:PYTHONPATH = "."

uv run python src/main.py

### Pour le Downloader

Récupérer la clé API sur kaggle créer son compte et aller dans les paramètres , récupérer également son username placer les crédentials dans .env
Pour les exercices il faut créer un compte sur https://rapidapi.com/hub pour avoir une clé API (suivre le env.example)
se rendre sur API EDB : https://rapidapi.com/ascendapi/api/edb-with-videos-and-images-by-ascendapi/playground/apiendpoint_bafbc96b-3f58-4a76-aad0-6f8bc44d3afb

- uv run python src/data_pipeline/downloader/api_client.py

### Notebook

Pour lire les données recueillies dans Raw et faire une analyse
Selectionner le kernel en haut a droite python 3.13 (stable pour utiliser pandas etc) pour pouvoir activer les cellules. UV nous a proposé directement la version 3.14 de python mais elle n'est pas stable je m'en suis rendu compte j'ai utiliser la commande : uv python pin 3.13 fermé vscode et ouvert et selectionné le kernel 3.13

### Linter Ruff

- uv run ruff format --check -> check le format
- uv run ruff format . -> formate le code
- uv run ruff check -> check les erreurs
- uv run pytest -> run les test

### Lancement du server fast api manuellement

- uv run uvicorn src.server:app --reload
- URL d'accès au swagger : http://127.0.0.1:8000/docs
