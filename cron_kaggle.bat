@echo off
:: 1. Se déplacer dynamiquement dans le dossier du script
cd /d "%~dp0"

:: 2. Lancer la commande Docker (qui elle est universelle)
docker compose run --rm api uv run python src/data_pipeline/downloader/api_client.py >> logs\cron_kaggle.log 2>&1